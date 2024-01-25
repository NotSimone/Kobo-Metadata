import re
import string
from queue import Queue
from typing import Optional, Tuple
from urllib.parse import urlencode

from calibre import browser
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Option, Source, fixauthors
from calibre.utils.config_base import tweaks
from calibre.utils.date import parse_only_date
from calibre.utils.logging import Log
from lxml import html


class KoboMetadata(Source):
    name = "Kobo Metadata"
    author = "NotSimone"
    version = (1, 4, 0)
    minimum_calibre_version = (2, 82, 0)
    description = _("Downloads metadata and covers from Kobo")

    capabilities = frozenset(("identify", "cover"))
    touched_fields = frozenset(
        (
            "title",
            "authors",
            "comments",
            "publisher",
            "pubdate",
            "languages",
            "series",
            "tags",
        )
    )
    has_html_comments = True
    supports_gzip_transfer_encoding = True

    BASE_URL = "https://www.kobo.com/"

    COUNTRIES = {
        "ca": _("Canada"),
        "us": _("United States"),
        "in": _("India"),
        "za": _("South Africa"),
        "au": _("Australia"),
        "hk": _("Hong Kong"),
        "ja": _("Japan"),
        "my": _("Malaysia"),
        "nz": _("New Zealand"),
        "ph": _("Phillipines"),
        "sg": _("Singapore"),
        "tw": _("Taiwan"),
        "th": _("Thailand"),
        "at": _("Austria"),
        "be": _("Belgium"),
        "cy": _("Cyprus"),
        "cz": _("Czech Republic"),
        "dk": _("Denmark"),
        "ee": _("Estonia"),
        "fi": _("Finland"),
        "fr": _("France"),
        "de": _("Germany"),
        "gr": _("Greece"),
        "ie": _("Ireland"),
        "it": _("Italy"),
        "lt": _("Lithuania"),
        "lu": _("Luxemburg"),
        "mt": _("Malta"),
        "nl": _("Netherlands"),
        "no": _("Norway"),
        "pl": _("Poland"),
        "pt": _("Portugal"),
        "ro": _("Romania"),
        "sk": _("Slovak Republic"),
        "si": _("Slovenia"),
        "es": _("Spain"),
        "se": _("Sweden"),
        "ch": _("Switzerland"),
        "tr": _("Turkey"),
        "gb": _("United Kingdom"),
        "br": _("Brazil"),
        "mx": _("Mexico"),
        "ww": _("Other"),
    }

    options = (
        Option(
            "country",
            "choices",
            "us",
            _("Kobo country store to use"),
            _("Metadata from Kobo will be fetched from this store"),
            choices=COUNTRIES,
        ),
        Option(
            "num_matches",
            "number",
            1,
            _("Number of matches to fetch"),
            _(
                "How many possible matches to fetch metadata for. If applying metadata in bulk, "
                "there is no use setting this above 1. Otherwise, set this higher if you are "
                "having trouble matching a specific book."
            ),
        ),
        Option(
            "title_blacklist",
            "string",
            "",
            _("Blacklist words in the title"),
            _("Comma separated words to blacklist"),
        ),
        Option(
            "tag_blacklist",
            "string",
            "",
            _("Blacklist tags"),
            _("Comma separated tags to blacklist"),
        ),
        Option(
            "remove_leading_zeroes",
            "bool",
            False,
            _("Remove leading zeroes"),
            _("Remove leading zeroes from numbers in the title"),
        ),
        Option(
            "resize_cover",
            "bool",
            False,
            _("Resize cover"),
            _("Resize the cover to the maximum_cover_size tweak setting"),
        ),
    )

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)

    def get_book_url(self, identifiers) -> Optional[Tuple]:
        isbn = identifiers.get("isbn", None)
        if isbn:
            # Example output:"https://www.kobo.com/au/en/search?query=9781761108105"
            return ("isbn", isbn, self._get_search_url(isbn, 1))
        return None

    def get_cached_cover_url(self, identifiers) -> Optional[str]:
        isbn = identifiers.get("isbn", None)

        if isbn is not None:
            return self.cached_identifier_to_cover_url(isbn)

        return None

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers={},
        timeout=30,
    ) -> None:
        log.info(f"KoboMetadata::identify: title: {title}, authors: {authors}, identifiers: {identifiers}")

        isbn = check_isbn(identifiers.get("isbn", None))
        urls = []

        if isbn:
            log.info(f"KoboMetadata::identify: Getting metadata with isbn: {isbn}")
            # isbn searches will (sometimes) redirect to the product page
            isbn_urls = self._perform_query(isbn, log, timeout)
            if isbn_urls:
                urls.append(isbn_urls[0])

        query = self._generate_query(title, authors)
        log.info(f"KoboMetadata::identify: Searching with query: {query}")
        urls.extend(self._perform_query(query, log, timeout))

        index = 0
        for url in urls:
            log.info(f"KoboMetadata::identify: Looking up metadata with url: {url}")
            try:
                metadata = self._lookup_metadata(url, log, timeout)
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

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers={},
        timeout=30,
        get_best_cover=False,
    ) -> None:
        cover_url = self.get_cached_cover_url(identifiers)
        if not cover_url:
            log.info("KoboMetadata::download_cover: No cached url found, running identify")
            res_queue = Queue()
            self.identify(log, res_queue, abort, title, authors, identifiers, timeout)
            if res_queue.empty():
                log.error("KoboMetadata::download_cover: Could not identify book")
                return

            metadata = res_queue.get()
            cover_url = self.get_cached_cover_url(metadata)
        if not cover_url:
            log.error("KoboMetadata::download_cover: Could not find cover")

        br = self._get_browser()
        try:
            cover = br.open_novisit(cover_url, timeout=timeout).read()
        except Exception as e:
            log.error(f"KoboMetadata::download_cover: Got exception while opening cover url: {e}")
            return

        result_queue.put((self, cover))

    def _get_search_url(self, search_str: str, page_number: int) -> str:
        query = {"query": search_str, "fcmedia": "Book", "pageNumber": page_number}
        return f"{self.BASE_URL}{self.prefs['country']}/en/search?{urlencode(query)}"

    def _generate_query(self, title: str, authors: list[str]) -> str:
        # Remove leading zeroes from the title if configured
        # Kobo search doesn't do a great job of matching numbers
        title = " ".join(
            x.lstrip("0") if self.prefs["remove_leading_zeroes"] else x
            for x in self.get_title_tokens(title, strip_joiners=False, strip_subtitle=False)
        )

        if authors:
            title += " " + " ".join(self.get_author_tokens(authors))

        return title

    def _get_browser(self) -> browser:
        br: browser = self.browser
        br.set_header(
            "User-Agent",
            "Mozilla/5.0 (Linux; Android 8.0.0; VTR-L29; rv:63.0) Gecko/20100101 Firefox/63.0",
        )
        return br

    # Returns [lxml html element, is search result]
    def _get_webpage(self, url: str, log: Log, timeout: int) -> Tuple[Optional[html.Element], bool]:
        br = self._get_browser()
        try:
            raw = br.open_novisit(url, timeout=timeout).read()
            tree = html.fromstring(raw)
            is_search = len(tree.xpath("//div[@class='search-results-display kobo-gizmo']")) != 0
            return (tree, is_search)
        except Exception as e:
            log.error(f"KoboMetadata::_get_webpage: Got exception while opening url: {e}")
            return (None, False)

    # Returns a list of urls that match our search
    def _perform_query(self, query: str, log: Log, timeout: int) -> list[str]:
        url = self._get_search_url(query, 1)
        log.info(f"KoboMetadata::identify: Searching for book with url: {url}")

        tree, is_search = self._get_webpage(url, log, timeout)
        if tree is None:
            log.info(f"KoboMetadata::_lookup_metadata: Could not get url: {url}")
            return []

        # Query redirected straight to product page
        if not is_search:
            return [url]

        search_results_elements = tree.xpath("//h2[@class='title product-field']/a")
        results = [x.get("href") for x in search_results_elements]

        page_num = 2
        while len(results) < self.prefs["num_matches"]:
            url = self._get_search_url(query, page_num)
            tree, is_search = self._get_webpage(url, log, timeout)
            assert tree and is_search
            search_results_elements = tree.xpath("//h2[@class='title product-field']/a")
            results.extend([x.get("href") for x in search_results_elements])
            page_num += 1

        return results[: self.prefs["num_matches"]]

    # Given the url for a book, parse and return the metadata
    def _lookup_metadata(self, url: str, log: Log, timeout: int) -> Optional[Metadata]:
        tree, is_search = self._get_webpage(url, log, timeout)
        if tree is None or is_search:
            log.info(f"KoboMetadata::_lookup_metadata: Could not get url: {url}")
            return None

        title_elements = tree.xpath("//h1[@class='title product-field']")
        title = title_elements[0].text.strip()
        log.info(f"KoboMetadata::_lookup_metadata: Got title: {title}")

        authors_elements = tree.xpath("//span[@class='visible-contributors']/a")
        authors = fixauthors([x.text for x in authors_elements])
        log.info(f"KoboMetadata::_lookup_metadata: Got authors: {authors}")

        metadata = Metadata(title, authors)

        series_elements = tree.xpath("//span[@class='series product-field']")
        if series_elements:
            # Books in series but without an index get a nested series product-field class
            # With index: https://www.kobo.com/au/en/ebook/fourth-wing-1
            # Without index: https://www.kobo.com/au/en/ebook/les-damnees-de-la-mer-femmes-et-frontieres-en-mediterranee
            series_name_element = series_elements[-1].xpath("span[@class='product-sequence-field']/a")
            if series_name_element:
                metadata.series = series_name_element[0].text
                log.info(f"KoboMetadata::_lookup_metadata: Got series: {metadata.series}")

            series_index_element = series_elements[-1].xpath("span[@class='sequenced-name-prefix']")
            if series_index_element:
                series_index_match = re.match("Book (.*) - ", series_index_element[0].text)
                if series_index_match:
                    metadata.series_index = series_index_match.groups(0)[0]
                    log.info(f"KoboMetadata::_lookup_metadata: Got series_index: {metadata.series_index}")

        book_details_elements = tree.xpath("//div[@class='bookitem-secondary-metadata']/ul/li")
        if book_details_elements:
            metadata.publisher = book_details_elements[0].text.strip()
            log.info(f"KoboMetadata::_lookup_metadata: Got publisher: {metadata.publisher}")
            for x in book_details_elements[1:]:
                descriptor = x.text.strip()
                if descriptor == "Release Date:":
                    metadata.pubdate = parse_only_date(x.xpath("span")[0].text)
                    log.info(f"KoboMetadata::_lookup_metadata: Got pubdate: {metadata.pubdate}")
                elif descriptor == "ISBN:":
                    metadata.isbn = x.xpath("span")[0].text
                    log.info(f"KoboMetadata::_lookup_metadata: Got isbn: {metadata.isbn}")
                elif descriptor == "Language:":
                    metadata.language = x.xpath("span")[0].text
                    log.info(f"KoboMetadata::_lookup_metadata: Got language: {metadata.language}")

        tags_elements = tree.xpath("//ul[@class='category-rankings']/meta[@property='genre']")
        if tags_elements:
            # Calibre doesnt like commas in tags
            metadata.tags = {x.get("content").replace(", ", " ") for x in tags_elements}
            log.info(f"KoboMetadata::_lookup_metadata: Got tags: {metadata.tags}")

        synopsis_elements = tree.xpath("//div[@class='synopsis-description']")
        if synopsis_elements:
            metadata.comments = html.tostring(synopsis_elements[0], method="html")
            log.info(f"KoboMetadata::_lookup_metadata: Got comments: {metadata.comments}")

        cover_elements = tree.xpath("//img[contains(@class, 'cover-image')]")
        if cover_elements:
            # Sample: https://cdn.kobo.com/book-images/44f0e8b9-3338-4d1c-bd6e-e88e82cb8fad/353/569/90/False/holly-23.jpg
            cover_url = "https:" + cover_elements[0].get("src")
            if self.prefs["resize_cover"]:
                # Change the resolution from 353x569 to maximum_cover_size (default 1650x2200)
                # Kobo will resize to match the width and have the correct aaspect ratio
                width, height = tweaks["maximum_cover_size"]
                cover_url = cover_url.replace("353/569/90", f"{width}/{height}/100")
            else:
                # Removing this gets the original cover art (probably)
                # Sample: https://cdn.kobo.com/book-images/44f0e8b9-3338-4d1c-bd6e-e88e82cb8fad/holly-23.jpg
                cover_url = cover_url.replace("353/569/90/False/", "")
            self.cache_identifier_to_cover_url(metadata.isbn, cover_url)
            log.info(f"KoboMetadata::_lookup_metadata: Got cover: {cover_url}")

        blacklisted_title = self._check_title_blacklist(title, log)
        if blacklisted_title:
            log.info(f"KoboMetadata::_lookup_metadata: Hit blacklisted word(s) in the title: {blacklisted_title}")
            return None

        blacklisted_tags = self._check_tag_blacklist(metadata.tags, log)
        if blacklisted_tags:
            log.info(f"KoboMetadata::_lookup_metadata: Hit blacklisted tag(s): {blacklisted_tags}")
            return None

        return metadata

    # Returns the set of words in the title that are also blacklisted
    def _check_title_blacklist(self, title: str, log: Log) -> set[str]:
        if not self.prefs["title_blacklist"]:
            return None

        blacklisted_words = {x.strip().lower() for x in self.prefs["title_blacklist"].split(",")}
        log.info(f"KoboMetadata::_check_title_blacklist: blacklisted title words: {blacklisted_words}")
        # Remove punctuation from title string
        title_str = title.translate(str.maketrans("", "", string.punctuation))
        return blacklisted_words.intersection(title_str.lower().split(" "))

    # Returns the set of tags that are also blacklisted
    def _check_tag_blacklist(self, tags: set[str], log: Log) -> set[str]:
        if not self.prefs["tag_blacklist"]:
            return None

        blacklisted_tags = {x.strip().lower() for x in self.prefs["tag_blacklist"].split(",")}
        log.info(f"KoboMetadata::_check_tag_blacklist: blacklisted tags: {blacklisted_tags}")
        return blacklisted_tags.intersection({x.lower() for x in tags})
