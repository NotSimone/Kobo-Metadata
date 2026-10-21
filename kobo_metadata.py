import json
import os
import pathlib
import re
import sqlite3
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
    # ISO 639-1 codes of the languages Kobo offers in its search filter
    KOBO_LANGUAGES = frozenset({"de", "en", "es", "fr", "it", "ja", "nl", "pt", "tr", "zh"})
    session: Optional[requests.Session] = None

    def __init__(self, plugin):
        self.plugin = plugin

    def get_search_url(self, search_str: str, page_number: int, prefs: Dict[str, any]) -> str:
        query = {"query": search_str, "fcmedia": "Book", "pageNumber": page_number, "fclanguages": prefs["language"]}
        if prefs["only_light_novels"] and prefs["language"] == "ja" and prefs["country"] == "jp":
            query["id"] = "dac63710-e136-4309-ac7f-d517e2202171"  # Kobo tag for light novels in the Japanese store
        return f"{self.BASE_URL}{prefs['country']}/{prefs['language']}/search?{urlencode(query)}"

    def get_kobo_url(self, kobo_id: str, prefs: Dict[str, any]) -> str:
        if prefs["language"] == "all":
            url = f"{self.BASE_URL}{prefs['country']}/search?query={kobo_id}"
        else:
            url = f"{self.BASE_URL}{prefs['country']}/{prefs['language']}/search?query={kobo_id}"
        return url

    @staticmethod
    def check_isbn_valid(isbn: str) -> bool:
        if not isbn:
            return False
        return isbn[:3] == "978" or isbn[:3] == "979"

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
        prefs = self._book_language_prefs(title, authors, identifiers, prefs, log)

        id_urls = []
        isbn = check_isbn(identifiers.get("isbn", None))
        kobo = identifiers.get("kobo", None)

        if kobo:
            log.info(f"Searching with Kobo ID: {kobo}")
            id_urls.append(self.get_kobo_url(kobo, prefs))

        if isbn:
            log.info(f"Searching with ISBN: {isbn}")
            id_urls.append(self.get_kobo_url(isbn, prefs))

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
        prefs = self._book_language_prefs(title, authors, identifiers, prefs, log)
        urls = []

        isbn = check_isbn(identifiers.get("isbn", None))
        if isbn:
            urls.extend(self._perform_isbn_search(isbn, 1, prefs, timeout, log))

        kobo = identifiers.get("kobo", None)
        if kobo:
            urls.append(self.get_kobo_url(kobo, prefs))

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

        if authors and prefs["use_author"]:
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
        # Kobo is moving their book pages to a new layout, starting with the Japanese store
        if page.xpath("//h1[@data-testid='book-title']"):
            log.info("KoboMetadata::parse_book_page: Detected new book page")
            metadata = self._parse_new_book_page(page, prefs, log)
        else:
            metadata = self._parse_old_book_page(page, prefs, log)

        cover_url = self._parse_book_page_for_cover(page, prefs, log)
        identifier = metadata.isbn if metadata.isbn else metadata.identifiers.get("kobo", None)
        if cover_url and identifier:
            self.plugin.cache_identifier_to_cover_url(identifier, cover_url)

        blacklisted_title = self._check_title_blacklist(metadata.title, prefs, log)
        if blacklisted_title:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted word(s) in the title: {blacklisted_title}")
            return None

        blacklisted_tags = self._check_tag_blacklist(metadata.tags, prefs, log)
        if blacklisted_tags:
            log.info(f"KoboMetadata::parse_book_page: Hit blacklisted tag(s): {blacklisted_tags}")
            return None

        return metadata

    def _parse_old_book_page(self, page: html.Element, prefs: Dict[str, any], log: Log) -> Metadata:
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
                series_index_match = re.match("(本:|Book) (.*) - ", series_index_element[0].text)
                if series_index_match:
                    metadata.series_index = series_index_match.groups(0)[1]
                    log.info(f"KoboMetadata::parse_book_page: Got series_index: {metadata.series_index}")

        book_details_elements = page.xpath("//div[@class='bookitem-secondary-metadata']/ul/li")
        if book_details_elements:
            metadata.publisher = book_details_elements[0].text.strip()
            log.info(f"KoboMetadata::parse_book_page: Got publisher: {metadata.publisher}")
            for x in book_details_elements[1:]:
                descriptor = x.text.strip()
                data = x.xpath("span")[0].text if x.xpath("span") else None

                if descriptor == "Release Date:" or descriptor == "発売日：":  # japanese colon is different
                    if prefs["language"] == "ja":
                        date_match = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", data)
                        data = f"{date_match.groups(0)[0]}-{date_match.groups(0)[1]}-{date_match.groups(0)[2]}"
                    if data:
                        metadata.pubdate = parse_only_date(data)
                        log.info(f"KoboMetadata::parse_book_page: Got pubdate: {metadata.pubdate}")

                elif descriptor == "ISBN:" or descriptor == "Book ID:" or descriptor == "書籍ID：":
                    self._set_isbn(metadata, data, prefs, log)

                elif descriptor == "Language:" or descriptor == "言語：":
                    if prefs["language"] == "ja":
                        # Assuming JP store will be mainly used for JP books, otherwise to much baggage to add all languages
                        data = "Japanese" if data == "日本語" else "English"
                    metadata.language = data
                    log.info(f"KoboMetadata::parse_book_page: Got language: {metadata.language}")

        tags_elements = page.xpath("//ul[@class='category-rankings']/meta[@property='genre']")
        if tags_elements:
            # Calibre doesnt like commas in tags
            metadata.tags = {x.get("content").replace(", ", " ") for x in tags_elements}
            log.info(f"KoboMetadata::parse_book_page: Got tags: {metadata.tags}")

        synopsis_elements = page.xpath("//div[@data-full-synopsis='']")
        if synopsis_elements:
            metadata.comments = "".join(html.tostring(child, encoding="unicode") for child in synopsis_elements[0])
            log.info(f"KoboMetadata::parse_book_page: Got comments: {metadata.comments}")

        return metadata

    # The new book pages are built with Next.js. The title, authors and cover are read from the html. The rest comes
    # from the data embedded in the page: the synopsis is only filled in by javascript, the "About this book" section
    # is localised (and not always correctly, e.g. it has Turkish labels on the Japanese store), and the genre chips
    # leave out the parent categories that the tags on the old book page included.
    def _parse_new_book_page(self, page: html.Element, prefs: Dict[str, any], log: Log) -> Metadata:
        title = page.xpath("//h1[@data-testid='book-title']")[0].text_content().strip()
        log.info(f"KoboMetadata::parse_new_book_page: Got title: {title}")

        authors_elements = page.xpath("//div[@data-testid='book-metadata-details']//a[contains(@href, '/author/')]")
        authors = fixauthors([x.text_content().strip() for x in authors_elements])
        log.info(f"KoboMetadata::parse_new_book_page: Got authors: {authors}")

        metadata = Metadata(title, authors)

        try:
            item_details = self._get_item_details(page)
            if not item_details:
                log.error("KoboMetadata::parse_new_book_page: Could not find the book details in the page data")
        except Exception as e:
            log.error(f"KoboMetadata::parse_new_book_page: Got exception reading the page data: {e}")
            item_details = None

        tags = None
        if item_details:
            # Use the language the page is in (e.g. ja-JP), like the genre chips do
            locale = page.xpath("string(/html/@lang)").lower() or prefs["language"]
            tags = self._get_category_names(item_details, locale)
        if not tags:
            # The genre chips only show the last category of each of the book's category paths
            tags = {x.text_content().strip() for x in page.xpath("//ul[@data-testid='categories-item-details']//a")}
            tags.discard("")
        if tags:
            # Calibre doesnt like commas in tags
            metadata.tags = {x.replace(", ", " ") for x in tags}
            log.info(f"KoboMetadata::parse_new_book_page: Got tags: {metadata.tags}")

        if not item_details:
            return metadata

        book = item_details["metadata"]

        series = book.get("series")
        if isinstance(series, dict) and series.get("name"):
            metadata.series = series["name"]
            log.info(f"KoboMetadata::parse_new_book_page: Got series: {metadata.series}")
            try:
                metadata.series_index = float(series.get("number"))
                log.info(f"KoboMetadata::parse_new_book_page: Got series_index: {metadata.series_index}")
            except (TypeError, ValueError):
                pass

        if book.get("isbn"):
            self._set_isbn(metadata, book["isbn"], prefs, log)

        # Sample: 2020-03-14T00:00:00.0000000Z
        if book.get("publicationDate"):
            metadata.pubdate = parse_only_date(book["publicationDate"][:10])
            log.info(f"KoboMetadata::parse_new_book_page: Got pubdate: {metadata.pubdate}")

        publisher = item_details.get("publisher")
        if isinstance(publisher, dict) and publisher.get("publisher"):
            metadata.publisher = publisher["publisher"]
            log.info(f"KoboMetadata::parse_new_book_page: Got publisher: {metadata.publisher}")

        # The English name of the language (e.g. Japanese), even on the Japanese store
        if book.get("language"):
            metadata.language = book["language"]
            log.info(f"KoboMetadata::parse_new_book_page: Got language: {metadata.language}")

        if book.get("description"):
            metadata.comments = book["description"]
            log.info(f"KoboMetadata::parse_new_book_page: Got comments: {metadata.comments}")

        return metadata

    # Next.js embeds the data used to render the page as a stream of rows, split across self.__next_f.push([1, "..."])
    # calls. Returns the "itemDetails" object that describes the book, or None if it could not be found.
    def _get_item_details(self, page: html.Element) -> Optional[Dict[str, any]]:
        payload = ""
        for script in page.xpath("//script[contains(text(), 'self.__next_f.push(')]"):
            for match in re.finditer(r"self\.__next_f\.push\(\s*", script.text):
                chunk, _ = json.JSONDecoder().raw_decode(script.text, match.end())
                if len(chunk) > 1 and chunk[0] == 1:
                    payload += chunk[1]

        # Rows end with a newline, except for text rows ("<id>:T<length in bytes>,<text>"). These hold long strings
        # (such as most English synopses), which are then referenced elsewhere as "$<id>".
        text_rows = {}
        data = payload.encode("utf-8")
        text_row_header = re.compile(rb"([0-9a-f]+):T([0-9a-f]+),")
        pos = 0
        while pos < len(data):
            match = text_row_header.match(data, pos)
            if match:
                pos = match.end() + int(match.group(2), 16)
                text_rows[match.group(1).decode()] = data[match.end() : pos].decode("utf-8", errors="replace")
            else:
                newline = data.find(b"\n", pos)
                pos = len(data) if newline == -1 else newline + 1

        def resolve_references(obj: Dict[str, any]) -> Dict[str, any]:
            return {key: self._resolve_page_data_value(value, text_rows) for key, value in obj.items()}

        decoder = json.JSONDecoder(object_hook=resolve_references)
        for match in re.finditer(r'"itemDetails":\s*', payload):
            item_details, _ = decoder.raw_decode(payload, match.end())
            if isinstance(item_details, dict) and isinstance(item_details.get("metadata"), dict):
                return item_details
        return None

    # Strings in the page data that start with "$" are references, such as "$3a" for the text row with id 3a, or
    # "$undefined". Strings that really start with "$" are escaped as "$$".
    @staticmethod
    def _resolve_page_data_value(value: any, text_rows: Dict[str, str]) -> any:
        if not isinstance(value, str) or not value.startswith("$"):
            return value
        if value.startswith("$$"):
            return value[1:]
        return text_rows.get(value[1:])

    # Returns the name of every category along the paths of the book's categories (e.g. 小説・文学 > ライトノベル), which
    # is what the tags on the old book page were made of. Returns an empty set if the categories can't be read.
    def _get_category_names(self, item_details: Dict[str, any], locale: str) -> set[str]:
        names = set()
        try:
            for path in item_details["browseCategories"]:
                for category in path["categories"]:
                    names.add(self._get_localised_name(category["name"], locale))
        except (KeyError, TypeError, AttributeError):
            return set()
        return {name for name in names if name}

    # Category names are in English, with translations keyed by language or by language and region (e.g. ja, zh-TW)
    @staticmethod
    def _get_localised_name(name: Dict[str, any], locale: str) -> Optional[str]:
        translations = {key.lower(): value for key, value in (name.get("translations") or {}).items() if value}
        language = locale.split("-")[0]
        for key in (locale, language):
            if key in translations:
                return translations[key]
        for key, value in translations.items():
            if key.startswith(f"{language}-"):
                return value
        return name.get("default")

    # Kobo lists its own book id in place of the isbn for some books (e.g. on the Japanese store)
    def _set_isbn(self, metadata: Metadata, isbn: str, prefs: Dict[str, any], log: Log) -> None:
        if prefs["differentiate_kobo_isbn"] and not KoboMetadataImpl.check_isbn_valid(isbn):
            metadata.set_identifier("kobo", isbn)
            log.info(f"KoboMetadata::set_isbn: Got kobo book id: {isbn}")
        else:
            metadata.isbn = isbn
            log.info(f"KoboMetadata::set_isbn: Got isbn: {metadata.isbn}")

    def _parse_book_page_for_cover(self, page: html.Element, prefs: Dict[str, any], log: Log) -> str:
        cover_url = None
        # Old and new book page layouts respectively
        cover_elements = page.xpath("//img[contains(@class, 'cover-image')]") or page.xpath(
            "//div[@data-testid='book-metadata-details']//img[@data-testid='cover']"
        )
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

    # --- Book language detection --------------------------------------------

    # Returns a copy of prefs with the detected book language, or the original
    # prefs if the language cannot be determined or auto-detection is disabled.
    def _book_language_prefs(
        self, title: str, authors: List[str], identifiers: Dict[str, any], prefs: Dict[str, any], log: Log
    ) -> Dict[str, any]:
        if not prefs["use_book_language"]:
            return prefs

        lang = self._detect_book_language(title, authors, identifiers, log)
        if not lang:
            return prefs

        merged = {**prefs.defaults, **prefs, "language": lang}
        log.info(f"KoboMetadata::_book_language_prefs: overriding search language to '{lang}'")
        return merged

    # Returns the ISO 639-1 (2 letter) code of the book's language, or None.
    # Calibre runs metadata downloads in a separate worker process without
    # access to the GUI or the library, and identify() only receives title,
    # authors and identifiers. So the language is read directly from the
    # metadata.db of the library the GUI last opened, in read-only mode.
    def _detect_book_language(
        self, title: str, authors: List[str], identifiers: Dict[str, any], log: Log
    ) -> Optional[str]:
        try:
            from calibre.utils.config import prefs as calibre_prefs

            library_path = calibre_prefs["library_path"]
        except Exception as e:
            log.info(f"KoboMetadata::_detect_book_language: could not determine library path: {e}")
            return None

        if not library_path:
            return None

        db_path = os.path.join(library_path, "metadata.db")
        if not os.path.exists(db_path):
            log.info(f"KoboMetadata::_detect_book_language: no library database at {db_path}")
            return None

        try:
            languages = self._query_book_languages(db_path, title, authors, identifiers)
        except Exception as e:
            log.info(f"KoboMetadata::_detect_book_language: could not read library database: {e}")
            return None

        if len(languages) != 1:
            log.info(f"KoboMetadata::_detect_book_language: no unique language found in library: {languages}")
            return None

        from calibre.utils.localization import lang_as_iso639_1

        lang = languages.pop()
        code = lang_as_iso639_1(lang)
        if code not in self.KOBO_LANGUAGES:
            log.info(f"KoboMetadata::_detect_book_language: language '{lang}' is not available on Kobo")
            return None

        log.info(f"KoboMetadata::_detect_book_language: detected '{code}' from '{lang}'")
        return code

    # Returns the set of primary languages of all library books that match by
    # identifier, or by title and author, in a single query.
    def _query_book_languages(
        self, db_path: str, title: str, authors: List[str], identifiers: Dict[str, any]
    ) -> set:
        conditions = []
        params = []
        for key, value in (identifiers or {}).items():
            value = str(value).strip()
            if key == "isbn":
                value = check_isbn(value)
            if value:
                conditions.append("b.id IN (SELECT book FROM identifiers WHERE type = ? AND val = ?)")
                params += [key, value]

        # Calibre stores commas in author names as '|'
        author_names = [a.replace(",", "|") for a in (authors or []) if a]
        if title and author_names:
            placeholders = ", ".join("?" * len(author_names))
            conditions.append(
                "(b.title = ? COLLATE NOCASE AND b.id IN ("
                "SELECT bal.book FROM books_authors_link bal JOIN authors a ON a.id = bal.author "
                f"WHERE a.name COLLATE NOCASE IN ({placeholders})))"
            )
            params += [title, *author_names]

        if not conditions:
            return set()

        query = (
            "SELECT b.id, l.lang_code FROM books b "
            "JOIN books_languages_link bll ON bll.book = b.id "
            "JOIN languages l ON l.id = bll.lang_code "
            f"WHERE {' OR '.join(conditions)} "
            "ORDER BY b.id, bll.item_order"
        )
        conn = sqlite3.connect(f"{pathlib.Path(db_path).as_uri()}?mode=ro", uri=True, timeout=5)
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

        # Only the first (primary) language of each matching book counts
        primary = {}
        for book_id, lang in rows:
            primary.setdefault(book_id, lang)
        return set(primary.values())
