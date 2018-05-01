class Citation(object):

    def __init__(self, short_name, authors, title, year, journal=None,
                 pages=None, volume=None, issue=None, institute=None,
                 month=None, proceedings=None, url=None, pdf=None,
                 doi=None):
        self._short_name = short_name
        self._authors = authors
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
            self._authors == other._authors and
            self._title == other._title and
            self._year == other._year and
            self._journal == other._journal and
            self._volume == other._volume and
            self._issue == other.issue and
            self._pages == other._pages and
            self._institute == other._institute and
            self._month == other._month and
            self._proceedings == other._proceedings and
            self._url == other._url and
            self._pdf == other._pdf and
            self._doi == other._doi)

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
