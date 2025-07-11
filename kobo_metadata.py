import re
import string
import time
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
        query = {"query": search_str, "fcmedia": "Book", "pageNumber": page_number, "fclanguages": prefs["language"]}
        return f"{self.BASE_URL}{prefs['country']}/{prefs['language']}/search?{urlencode(query)}"
    
    def get_kobo_url(self, kobo_id: str, prefs: Dict[str, any]) -> str:
        if prefs['language'] == 'all':
            url = f"{self.BASE_URL}{prefs['country']}/ebook/{kobo_id}"
        else:
            url = f"{self.BASE_URL}{prefs['country']}/{prefs['language']}/ebook/{kobo_id}"
        return url

    def identify(
        self,
        result_queue: Queue,
        title: str,
        authors: List[str],
        identifiers: Dict[str, any],
        prefs: Dict[str, any],
        timeout: int,
        log: Log,
    ) -> None:
        log.info(f"KoboMetadata::identify: title: {title}, authors: {authors}, identifiers: {identifiers}")

        id_urls = []
        isbn = check_isbn(identifiers.get("isbn", None))
        kobo = identifiers.get("kobo", None)

        if kobo:
            log.info(f"Searching with Kobo ID: {kobo}")
            id_urls.append(self.get_kobo_url(kobo, prefs))

        if isbn:
            log.info(f"Searching with ISBN: {isbn}")
            id_urls.extend(self._perform_isbn_search(isbn, prefs["num_matches"], prefs, timeout, log))

        if id_urls:
            unique_id_urls = list(dict.fromkeys(id_urls))
            fetched_metadata = self._fetch_metadata(unique_id_urls, prefs, timeout, log)

            if fetched_metadata:
                log.info(f"Found {len(fetched_metadata)} match(es) using identifiers. Prioritizing these results.")
                for metadata in fetched_metadata:
                    result_queue.put(metadata)
                return

        # If no identifiers were provided, or they yielded no results, fall back to a general search.
        log.info("No matches found with identifiers, falling back to general search.")
        search_urls = self._perform_search(title, authors, prefs["num_matches"], prefs, timeout, log)

        if search_urls:
            unique_search_urls = list(dict.fromkeys(search_urls))
            fetched_metadata = self._fetch_metadata(unique_search_urls, prefs, timeout, log)

            if fetched_metadata:
                log.info(f"Found {len(fetched_metadata)} match(es) using general search.")
                for metadata in fetched_metadata:
                    result_queue.put(metadata)

    def get_cover_url(
        self,
        title: str,
        authors: List[str],
        identifiers: Dict[str, any],
        prefs: Dict[str, any],
        timeout: int,
        log: Log,
    ) -> None:
        log.info(f"KoboMetadata::get_cover_url: title: {title}, authors: {authors}, identifiers: {identifiers}")
        urls = []

        isbn = check_isbn(identifiers.get("isbn", None))
        if isbn:
            urls.extend(self._perform_isbn_search(isbn, 1, prefs, timeout, log))

        # Only go looking for more matches if we couldn't match isbn
        if not urls:
            log.error("KoboMetadata::get_cover_url:: No identifier - performing search")
            urls.extend(self._perform_search(title, authors, 1, prefs, timeout, log))

        if not urls:
            log.error("KoboMetadata::get_cover_url:: No search results")
            return

        url = urls[0]
        page, is_search = self._get_webpage(url, timeout, log)
        if page is None or is_search:
            log.info(f"KoboMetadata::get_cover_url: Could not get url: {url}")
            return ""

        return self._parse_book_page_for_cover(page, prefs, log)

    def get_cover(self, cover_url: str, timeout: int) -> bytes:
        session = self._get_session()
        return session.get(cover_url, timeout=timeout).content

    def _get_session(self) -> requests.Session:
        if self.session is None:
            self.session = cloudscraper.create_scraper(
                browser={
                    "custom": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
                },
                interpreter="v8",
                ecdhCurve="secp384r1",
            )
        return self.session

    # Returns [lxml html element, is search result]
    def _get_webpage(self, url: str, timeout: int, log: Log) -> Tuple[Optional[html.Element], bool]:
        session = self._get_session()
        try:
            attempts = 0
            while attempts < 15:
                resp = session.get(url, timeout=timeout)
                page = html.fromstring(resp.text)
                # If we failed to get past the cloudflare protection, we get a page with one of these classes
                if (
                    not page.xpath("//form[@class='challenge-form']")
                    and not page.xpath("//form[@id='challenge-form']")
                    and not page.xpath("//span[@id='challenge-error-text']")
                ):
                    is_search = "/search?" in resp.url
                    return (page, is_search)
                log.info(f"KoboMetadata::get_webpage: Could not defeat cloudflare protection - trying again for {url}")
                attempts += 1
                time.sleep(1.0)
            log.error(f"KoboMetadata::get_webpage: Could not defeat cloudflare protection - giving up for {url}")
            return (None, False)
        except Exception as e:
            log.error(f"KoboMetadata::get_webpage: Got exception while opening url: {e}")
            return (None, False)

    def _perform_isbn_search(
        self, isbn: int, max_matches: int, prefs: Dict[str, any], timeout: int, log: Log
    ) -> List[str]:
        isbn = check_isbn(isbn)

        if isbn:
            log.info(f"KoboMetadata::perform_isbn_search: Getting metadata with isbn: {isbn}")
            return self._perform_query(isbn, max_matches, prefs, timeout, log)

    def _perform_search(
        self, title: str, authors: List[str], max_matches: int, prefs: Dict[str, any], timeout: int, log: Log
    ) -> List[str]:
        query = self._generate_query(title, authors, prefs)
        log.info(f"KoboMetadata::perform_search: Searching with query: {query}")
        return self._perform_query(query, max_matches, prefs, timeout, log)

    def _fetch_metadata(self, urls: List[str], prefs: Dict[str, any], timeout: int, log: Log) -> List[Metadata]:
        results = []
        index = 0
        for url in urls:
            log.info(f"KoboMetadata::fetch_metadata: Looking up metadata with url: {url}")
            try:
                page, is_search = self._get_webpage(url, timeout, log)
                if page is None or is_search:
                    log.info(f"KoboMetadata::fetch_metadata: Could not get url: {url}")
                    return
                metadata = self._parse_book_page(page, prefs, log)
            except Exception as e:
                log.error(f"KoboMetadata::fetch_metadata: Got exception looking up metadata: {e}")
                return

            if metadata:
                metadata.source_relevance = index
                results.append(metadata)
            else:
                log.info("KoboMetadata::fetch_metadata:: Could not find matching book")
            index += 1
        return results

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
    def _perform_query(self, query: str, max_matches: int, prefs: Dict[str, any], timeout: int, log: Log) -> list[str]:
        url = self.get_search_url(query, 1, prefs)
        log.info(f"KoboMetadata::perform_query: Searching for book with url: {url}")

        page, is_search = self._get_webpage(url, timeout, log)
        if page is None:
            log.info(f"KoboMetadata::perform_query: Could not get url: {url}")
            return []

        # Query redirected straight to product page
        if not is_search:
            return [url]

        results = self._parse_search_page(page, log)

        page_num = 2
        # a reasonable default for how many we should try before we give up
        max_page_num = 4
        while len(results) < max_matches and page_num < max_page_num:
            url = self.get_search_url(query, page_num, prefs)
            page, is_search = self._get_webpage(url, timeout, log)
            assert page and is_search
            results.extend(self._parse_search_page(page, log))
            page_num += 1

        return results[:max_matches]

    # Returns a list of urls on the search web page
    def _parse_search_page(self, page: html.Element, log: Log) -> List[str]:
        # Kobo seems to have partially moved to a new webpage for their search pages
        if len(page.xpath("//div[@data-testid='search-result-widget']")):
            log.info("KoboMetadata::parse_search_page: Detected new search page")
            result_elements = page.xpath("//a[@data-testid='title']")
            # Only get every second because the page includes mobile and web urls
            return [x.get("href") for x in result_elements[::2]]

        # Old
        result_elements = page.xpath("//h2[@class='title product-field']/a")
        if len(result_elements):
            log.info("KoboMetadata::parse_search_page: Detected old search page")
            return [x.get("href") for x in result_elements]

        log.error("KoboMetadata::parse_search_page: Found no matches or bad page")
        log.error(html.tostring(page))
        return []

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
                elif descriptor == "ISBN:" or descriptor == "Book ID:":
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

        synopsis_elements = page.xpath("//div[@data-full-synopsis='']")
        if synopsis_elements:
            metadata.comments = synopsis_elements[0].text_content()
            log.info(f"KoboMetadata::parse_book_page: Got comments: {metadata.comments}")

        cover_url = self._parse_book_page_for_cover(page, prefs, log)
        if cover_url:
            self.plugin.cache_identifier_to_cover_url(metadata.isbn, cover_url)

        blacklisted_title = self._check_title_blacklist(title, prefs, log)
        if blacklisted_title:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted word(s) in the title: {blacklisted_title}")
            return None

        blacklisted_tags = self._check_tag_blacklist(metadata.tags, prefs, log)
        if blacklisted_tags:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted tag(s): {blacklisted_tags}")
            return None

        return metadata

    def _parse_book_page_for_cover(self, page: html.Element, prefs: Dict[str, any], log: Log) -> str:
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

        log.info(f"KoboMetadata::parse_book_page_for_cover: Got cover: {cover_url}")
        return cover_url

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
