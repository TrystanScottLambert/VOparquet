"""
Testing the read module.
"""

import unittest
from read import extract_votable_metadata


class TestExtractVOTableMetadata(unittest.TestCase):
    """
    Testing the extraction of the xml data generaetes the correct dataframes.
    """

    def setUp(self):
        self.xml = """<?xml version="1.0"?>
        <VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
          <RESOURCE>
            <TABLE>
              <DESCRIPTION>Test VOTable Description</DESCRIPTION>
              <PARAM name="creator" value="TestUser" />
              <INFO name="version" value="1.0" />
              <COOSYS ID="J2000" equinox="2000.0" system="eq_FK5" />
              <FIELD name="ra" datatype="double" unit="deg" ucd="pos.eq.ra" arraysize="1">
                <DESCRIPTION>Right Ascension</DESCRIPTION>
              </FIELD>
              <FIELD name="dec" datatype="double" unit="deg" ucd="pos.eq.dec" arraysize="1">
                <DESCRIPTION>Declination</DESCRIPTION>
              </FIELD>
            </TABLE>
          </RESOURCE>
        </VOTABLE>
        """

    def test_extract_all_metadata(self):
        """
        testing that all the meta data gets extracted.
        """
        fields, params, infos, coosys, desc = extract_votable_metadata(self.xml)

        # Check DESCRIPTION
        self.assertEqual(desc, "Test VOTable Description")

        # Check FIELDs
        self.assertEqual(len(fields), 2)
        self.assertListEqual(fields["Name"].tolist(), ["ra", "dec"])
        self.assertListEqual(fields["Datatype"].tolist(), ["double", "double"])
        self.assertListEqual(
            fields["Description"].tolist(), ["Right Ascension", "Declination"]
        )

        # Check PARAM
        self.assertEqual(params.shape[0], 1)
        self.assertEqual(params.iloc[0]["name"], "creator")
        self.assertEqual(params.iloc[0]["value"], "TestUser")

        # Check INFO
        self.assertEqual(infos.shape[0], 1)
        self.assertEqual(infos.iloc[0]["name"], "version")
        self.assertEqual(infos.iloc[0]["value"], "1.0")

        # Check COOSYS
        self.assertEqual(coosys.shape[0], 1)
        self.assertEqual(coosys.iloc[0]["ID"], "J2000")
        self.assertEqual(coosys.iloc[0]["equinox"], "2000.0")
        self.assertEqual(coosys.iloc[0]["system"], "eq_FK5")


if __name__ == "__main__":
    unittest.main()
