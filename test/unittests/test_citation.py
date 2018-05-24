from unittest import TestCase
from arcana.citation import Citation


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
    pages=("S208", "S219"))


mrtrix_cite = Citation(
    short_name="mrtrix",
    authors=["Tournier, J-D"],
    title="MRtrix Package",
    institute="Brain Research Institute, Melbourne, Australia",
    url="https://github.com/MRtrix3/mrtrix3",
    year=2012)


class TestCitation(TestCase):

    def test_citation(self):
        self.assertEqual(fsl_cite, fsl_cite)
        self.assertEqual(fsl_cite, fsl_cite)
        self.assertNotEqual(fsl_cite, mrtrix_cite)
