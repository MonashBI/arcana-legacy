from builtins import str  # @UnusedImport
from builtins import object


class Citation(object):

    def __init__(self, short_name, authors, title, year, journal=None,
                 pages=None, volume=None, issue=None, institute=None,
                 month=None, proceedings=None, url=None, pdf=None,
                 doi=None):
        self._short_name = short_name
        self._authors = tuple(authors)
        self._title = title
        self._year = year
        self._journal = journal
        self._volume = volume
        self._issue = issue
        self._pages = pages
        self._institute = institute
        self._month = month
        self._proceedings = proceedings
        self._url = url
        self._pdf = pdf
        self._doi = doi

    def __eq__(self, other):
        return (
            self.short_name == other.short_name and
            self.authors == other.authors and
            self.title == other.title and
            self.year == other.year and
            self.journal == other.journal and
            self.volume == other.volume and
            self.issue == other.issue and
            self.pages == other.pages and
            self.institute == other.institute and
            self.month == other.month and
            self.proceedings == other.proceedings and
            self.url == other.url and
            self.pdf == other.pdf and
            self.doi == other.doi)

    def __hash__(self):
        return (hash(self.short_name) ^
                hash(self.authors) ^
                hash(self.title) ^
                hash(self.year) ^
                hash(self.journal) ^
                hash(self.volume) ^
                hash(self.issue) ^
                hash(self.pages) ^
                hash(self.institute) ^
                hash(self.month) ^
                hash(self.proceedings) ^
                hash(self.url) ^
                hash(self.pdf) ^
                hash(self.doi))

    def __ne__(self, other):
        return not (self == other)

    @property
    def short_name(self):
        return self._short_name

    @property
    def authors(self):
        return self._authors

    @property
    def title(self):
        return self._title

    @property
    def year(self):
        return self._year

    @property
    def journal(self):
        return self._journal

    @property
    def pages(self):
        return self._pages

    @property
    def volume(self):
        return self._volume

    @property
    def issue(self):
        return self._issue

    @property
    def institute(self):
        return self._institute

    @property
    def month(self):
        return self._month

    @property
    def proceedings(self):
        return self._proceedings

    @property
    def url(self):
        return self._url

    @property
    def pdf(self):
        return self._pdf

    @property
    def doi(self):
        return self._doi
