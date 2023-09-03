# Kobo Metadata

Metadata plugin for Calibre.

Features:
- Can grab title, author, synopsis, publisher, published date, series, and tags
- Can fetch high res cover art
- Blacklist tags / terms in the title

Installing:
- Preferences > Advanced > Plugins > Load Plugin fromm File > Select the zip
- Configure in Preferences > Sharing > Metadata Download > Select Kobo Metadata > Configure Selected Source

To fetch the metadata of multiple books at once, select some books, right click > Edit Metadata > Download Metadata and Covers. Note that this will pick the first match.

## Troubleshooting
- If the first match is not correct, try turning the option "Number of matches to fetch" in the plugin configuration higher and try to fetch metadata with the "Download metadata" button in the individual metadata editor. This will allow you to select from n number of possible matches.
- If you are having trouble matching a series, check what it is called on the Kobo store and try matching the titles.
- If you know what the correct match should be, try filling in the isbn in the Identifiers field with the format `isbn:xxxxxxxxxxxxx`. The plugin will then perform the metadata search with that isbn.
