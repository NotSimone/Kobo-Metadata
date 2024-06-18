import re
import string
from queue import Queue
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import fixauthors
from calibre.utils.config_base import tweaks
from calibre.utils.date import parse_only_date
from calibre.utils.logging import Log
from lxml import html

import cloudscraper
import requests


class KoboMetadataImpl:
    BASE_URL = "https://www.kobo.com/"
    session: Optional[requests.Session] = None

    def __init__(self, plugin):
        self.plugin = plugin

    def get_search_url(self, search_str: str, page_number: int, prefs: Dict[str, any]) -> str:
        query = {"query": search_str, "fcmedia": "Book", "pageNumber": page_number, "fclanguages": "all"}
        return f"{self.BASE_URL}{prefs['country']}/en/search?{urlencode(query)}"

    def identify(
        self,
        result_queue: Queue,
        title: Optional[str],
        authors: Optional[List[str]],
        identifiers: Dict[str, any],
        prefs: Dict[str, any],
        timeout: int,
        log: Log,
    ) -> None:
        log.info(f"KoboMetadata::identify: title: {title}, authors: {authors}, identifiers: {identifiers}")

        isbn = check_isbn(identifiers.get("isbn", None))
        urls = []

        if isbn:
            log.info(f"KoboMetadata::identify: Getting metadata with isbn: {isbn}")
            # isbn searches will (sometimes) redirect to the product page
            isbn_urls = self._perform_query(isbn, prefs, timeout, log)
            if isbn_urls:
                urls.extend(isbn_urls[0])

        query = self._generate_query(title, authors, prefs)
        log.info(f"KoboMetadata::identify: Searching with query: {query}")
        urls.extend(self._perform_query(query, prefs, timeout, log))

        index = 0
        for url in urls:
            log.info(f"KoboMetadata::identify: Looking up metadata with url: {url}")
            try:
                tree, is_search = self._get_webpage(url, timeout, log)
                if tree is None or is_search:
                    log.info(f"KoboMetadata::identify: Could not get url: {url}")
                    return
                metadata = self._parse_book_page(tree, prefs, log)
            except Exception as e:
                log.error(f"KoboMetadata::identify: Got exception looking up metadata: {e}")
                return

            if metadata:
                metadata.source_relevance = index
                result_queue.put(metadata)
            else:
                log.info("KoboMetadata::identify:: Could not find matching book")
            index += 1
        return

    def get_cover_url(
        self,
        title: Optional[str],
        authors: Optional[List[str]],
        identifiers: Dict[str, any],
        prefs: Dict[str, any],
        timeout: int,
        log: Log,
    ) -> None:
        log.info(f"KoboMetadata::get_cover_url: title: {title}, authors: {authors}, identifiers: {identifiers}")

        url = None
        isbn = check_isbn(identifiers.get("isbn", None))
        if isbn:
            log.info(f"KoboMetadata::get_cover_url: Getting metadata with isbn: {isbn}")
            # isbn searches will (sometimes) redirect to the product page
            isbn_urls = self._perform_query(isbn, prefs, timeout, log)
            if isbn_urls:
                url = isbn_urls[0]

        if not url:
            query = self._generate_query(title, authors, prefs)
            log.info(f"KoboMetadata::get_cover_url: Searching with query: {query}")
            results = self._perform_query(query, prefs, timeout, log)
            if not results:
                log.error("KoboMetadata::get_cover_url:: No search results")
                return
            else:
                url = results[0]

        tree, is_search = self._get_webpage(url, timeout, log)
        if tree is None or is_search:
            log.info(f"KoboMetadata::get_cover_url: Could not get url: {url}")
            return

        # Parsing the book page should be setting the cached url field
        self._parse_book_page(tree, prefs, log)

    def get_cover(self, cover_url: str, timeout: int) -> bytes:
        session = self._get_session()
        return session.get(cover_url, timeout=timeout).content

    def _get_session(self) -> requests.Session:
        if self.session is None:
            self.session = cloudscraper.create_scraper(
                browser={"browser": "firefox", "platform": "windows", "mobile": False}, interpreter="v8"
            )
        return self.session

    # Returns [lxml html element, is search result]
    def _get_webpage(self, url: str, timeout: int, log: Log) -> Tuple[Optional[html.Element], bool]:
        session = self._get_session()
        try:
            resp = session.get(url, timeout=timeout)
            tree = html.fromstring(resp.text)
            is_search = "/search?" in resp.url
            return (tree, is_search)
        except Exception as e:
            log.error(f"KoboMetadata::get_webpage: Got exception while opening url: {e}")
            return (None, False)

    def _generate_query(self, title: str, authors: list[str], prefs: Dict[str, any]) -> str:
        # Remove leading zeroes from the title if configured
        # Kobo search doesn't do a great job of matching numbers
        title = " ".join(
            x.lstrip("0") if prefs["remove_leading_zeroes"] else x
            for x in self.plugin.get_title_tokens(title, strip_joiners=False, strip_subtitle=False)
        )

        if authors:
            title += " " + " ".join(self.plugin.get_author_tokens(authors))

        return title

    # Returns a list of urls that match our search
    def _perform_query(self, query: str, prefs: Dict[str, any], timeout: int, log: Log) -> list[str]:
        url = self.get_search_url(query, 1, prefs)
        log.info(f"KoboMetadata::identify: Searching for book with url: {url}")

        tree, is_search = self._get_webpage(url, timeout, log)
        if tree is None:
            log.info(f"KoboMetadata::perform_query: Could not get url: {url}")
            return []

        # Query redirected straight to product page
        if not is_search:
            return [url]

        results = self._parse_search_page(tree, log)

        page_num = 2
        # a reasonable default for how many we should try before we give up
        max_page_num = 4
        while len(results) < prefs["num_matches"] and page_num < max_page_num:
            url = self.get_search_url(query, page_num, prefs)
            tree, is_search = self._get_webpage(url, timeout, log)
            assert tree and is_search
            results.extend(self._parse_search_page(tree, log))
            page_num += 1

        return results[: prefs["num_matches"]]

    # Returns a list of urls on the search web page
    def _parse_search_page(self, page: html.Element, log: Log) -> List[str]:
        # Kobo seems to have partially moved to a new webpage for their search pages
        if len(page.xpath("//div[@data-testid='search-result-widget']")):
            log.info("KoboMetadata::parse_search_page: Detected new search page")
            result_elements = page.xpath("//a[@data-testid='title']")
            # Only get every second because the page includes mobile and web urls
            return [x.get("href") for x in result_elements[::2]]

        # Old
        log.info("KoboMetadata::parse_search_page: Detected old search page")
        result_elements = page.xpath("//h2[@class='title product-field']/a")
        return [x.get("href") for x in result_elements]

    # Given a page that has the details of a book, parse and return the Metadata
    def _parse_book_page(self, page: html.Element, prefs: Dict[str, any], log: Log) -> Metadata:
        title_elements = page.xpath("//h1[@class='title product-field']")
        title = title_elements[0].text.strip()
        log.info(f"KoboMetadata::parse_book_page: Got title: {title}")

        authors_elements = page.xpath("//span[@class='visible-contributors']/a")
        authors = fixauthors([x.text for x in authors_elements])
        log.info(f"KoboMetadata::parse_book_page: Got authors: {authors}")

        metadata = Metadata(title, authors)

        series_elements = page.xpath("//span[@class='series product-field']")
        if series_elements:
            # Books in series but without an index get a nested series product-field class
            # With index: https://www.kobo.com/au/en/ebook/fourth-wing-1
            # Without index: https://www.kobo.com/au/en/ebook/les-damnees-de-la-mer-femmes-et-frontieres-en-mediterranee
            series_name_element = series_elements[-1].xpath("span[@class='product-sequence-field']/a")
            if series_name_element:
                metadata.series = series_name_element[0].text
                log.info(f"KoboMetadata::parse_book_page: Got series: {metadata.series}")

            series_index_element = series_elements[-1].xpath("span[@class='sequenced-name-prefix']")
            if series_index_element:
                series_index_match = re.match("Book (.*) - ", series_index_element[0].text)
                if series_index_match:
                    metadata.series_index = series_index_match.groups(0)[0]
                    log.info(f"KoboMetadata::parse_book_page: Got series_index: {metadata.series_index}")

        book_details_elements = page.xpath("//div[@class='bookitem-secondary-metadata']/ul/li")
        if book_details_elements:
            metadata.publisher = book_details_elements[0].text.strip()
            log.info(f"KoboMetadata::parse_book_page: Got publisher: {metadata.publisher}")
            for x in book_details_elements[1:]:
                descriptor = x.text.strip()
                if descriptor == "Release Date:":
                    metadata.pubdate = parse_only_date(x.xpath("span")[0].text)
                    log.info(f"KoboMetadata::parse_book_page: Got pubdate: {metadata.pubdate}")
                elif descriptor == "ISBN:":
                    metadata.isbn = x.xpath("span")[0].text
                    log.info(f"KoboMetadata::parse_book_page: Got isbn: {metadata.isbn}")
                elif descriptor == "Language:":
                    metadata.language = x.xpath("span")[0].text
                    log.info(f"KoboMetadata::parse_book_page: Got language: {metadata.language}")

        tags_elements = page.xpath("//ul[@class='category-rankings']/meta[@property='genre']")
        if tags_elements:
            # Calibre doesnt like commas in tags
            metadata.tags = {x.get("content").replace(", ", " ") for x in tags_elements}
            log.info(f"KoboMetadata::parse_book_page: Got tags: {metadata.tags}")

        synopsis_elements = page.xpath("//div[@class='synopsis-description']")
        if synopsis_elements:
            metadata.comments = html.tostring(synopsis_elements[0], method="html")
            log.info(f"KoboMetadata::parse_book_page: Got comments: {metadata.comments}")

        cover_elements = page.xpath("//img[contains(@class, 'cover-image')]")
        if cover_elements:
            # Sample: https://cdn.kobo.com/book-images/44f0e8b9-3338-4d1c-bd6e-e88e82cb8fad/353/569/90/False/holly-23.jpg
            cover_url = "https:" + cover_elements[0].get("src")
            if prefs["resize_cover"]:
                # Change the resolution from 353x569 to maximum_cover_size (default 1650x2200)
                # Kobo will resize to match the width and have the correct aaspect ratio
                width, height = tweaks["maximum_cover_size"]
                cover_url = cover_url.replace("353/569/90", f"{width}/{height}/100")
            else:
                # Removing this gets the original cover art (probably)
                # Sample: https://cdn.kobo.com/book-images/44f0e8b9-3338-4d1c-bd6e-e88e82cb8fad/holly-23.jpg
                cover_url = cover_url.replace("353/569/90/False/", "")
            self.plugin.cache_identifier_to_cover_url(metadata.isbn, cover_url)
            log.info(f"KoboMetadata::parse_book_page: Got cover: {cover_url}")

        blacklisted_title = self._check_title_blacklist(title, prefs, log)
        if blacklisted_title:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted word(s) in the title: {blacklisted_title}")
            return None

        blacklisted_tags = self._check_tag_blacklist(metadata.tags, prefs, log)
        if blacklisted_tags:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted tag(s): {blacklisted_tags}")
            return None

        return metadata

    # Returns the set of words in the title that are also blacklisted
    def _check_title_blacklist(self, title: str, prefs: Dict[str, any], log: Log) -> set[str]:
        if not prefs["title_blacklist"]:
            return None

        blacklisted_words = {x.strip().lower() for x in prefs["title_blacklist"].split(",")}
        log.info(f"KoboMetadata::_check_title_blacklist: blacklisted title words: {blacklisted_words}")
        # Remove punctuation from title string
        title_str = title.translate(str.maketrans("", "", string.punctuation))
        return blacklisted_words.intersection(title_str.lower().split(" "))

    # Returns the set of tags that are also blacklisted
    def _check_tag_blacklist(self, tags: set[str], prefs: Dict[str, any], log: Log) -> set[str]:
        if not prefs["tag_blacklist"]:
            return None

        blacklisted_tags = {x.strip().lower() for x in prefs["tag_blacklist"].split(",")}
        log.info(f"KoboMetadata::_check_tag_blacklist: blacklisted tags: {blacklisted_tags}")
        return blacklisted_tags.intersection({x.lower() for x in tags})
