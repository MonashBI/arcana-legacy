class Citation(object):

    def __init__(self, short_name, authors, title, year, journal=None,
                 pages=None, volume=None, institute=None, url=None):
        self._short_name = short_name
        self._authors = authors
        self._title = title
        self._year = year
        self._journal = journal
        self._volume = volume
        self._pages = pages
        self._institute = institute
        self._url = url

    def __eq__(self, other):
        return (
            self._authors == other._authors and
            self._title == other._title and
            self._year == other._year and
            self._journal == other._journal and
            self._volume == other._volume and
            self._pages == other._pages and
            self._institute == other._institute and
            self._url == other._url)

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
    def institute(self):
        return self._institute

    @property
    def url(self):
        return self._url


mrtrix_cite = Citation(
    short_name="mrtrix",
    authors=["Tournier, J-D"],
    title="MRtrix Package",
    institute="Brain Research Institute, Melbourne, Australia",
    url="https://github.com/MRtrix3/mrtrix3",
    year=2012)

eddy_cite = Citation(
    short_name='Eddy',
    authors=["Andersson, J. L.", "Sotiropoulos, S. N."],
    title=(
        "An integrated approach to correction for "
        "off-resonance effects and subject movement in "
        "diffusion MR imaging"),
    journal="NeuroImage", year=2015, volume=125,
    pages="1063-1078"),

fsl_cite = Citation(
    short_name="FSL",
    authors=["Smith, S. M.", "Jenkinson, M.",
             "Woolrich, M. W.", "Beckmann, C. F.",
             "Behrens, T. E.", "Johansen- Berg, H.",
             "Bannister, P. R.", "De Luca, M.", "Drobnjak, I.",
             "Flitney, D. E.", "Niazy, R. K.", "Saunders, J.",
             "Vickers, J.", "Zhang, Y.", "De Stefano, N.",
             "Brady, J. M. & Matthews, P. M."],
    title=(
        "Advances in functional and structural MR image "
        "analysis and implementation as FSL"),
    journal="NeuroImage", year=2004, volume=23,
    pages="S208-S219"),

distort_correct_cite = Citation(
    short_name="skare_distort_correct",
    authors=["Skare, S.", "Bammer, R."],
    title=(
        "Jacobian weighting of distortion corrected EPI data"),
    journal=(
        "Proceedings of the International Society for Magnetic"
        " Resonance in Medicine"), year=2010, pages="5063"),

topup_cite = Citation(
    short_name="Topup",
    authors=["Andersson, J. L.", "Skare, S. & Ashburner, J."],
    title=(
        "How to correct susceptibility distortions in "
        "spin-echo echo-planar images: application to "
        "diffusion tensor imaging"),
    journal="NeuroImage", year=2003, volume=20,
    pages="870-888")
