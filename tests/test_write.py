"""
Testing the write module.
"""

import unittest
import xml.etree.ElementTree as ET
import pandas as pd
from write import generate_votable_xml



class TestGenerateVOTableXML(unittest.TestCase):
    """
    Main testing class for the writing module.
    """

    def setUp(self):
        self.field_df = pd.DataFrame(
            [
                {
                    "Name": "ra",
                    "Unit": "deg",
                    "UCD": "pos.eq.ra",
                    "Datatype": "double",
                    "Arraysize": "1",
                    "Description": "Right Ascension",
                },
                {
                    "Name": "dec",
                    "Unit": "deg",
                    "UCD": "pos.eq.dec",
                    "Datatype": "double",
                    "Arraysize": "1",
                    "Description": "Declination",
                },
            ]
        )

        self.param_df = pd.DataFrame([{"name": "creator", "value": "TestUser"}])

        self.info_df = pd.DataFrame([{"name": "version", "value": "1.0"}])

        self.coosys_df = pd.DataFrame(
            [{"ID": "J2000", "equinox": "2000.0", "epoch": "2000", "system": "eq_FK5"}]
        )

    def test_generate_xml_structure(self):
        """
        Testing that the structure is generated correctly.
        """
        xml_str = generate_votable_xml(
            metadata_field_df=self.field_df,
            description="Test Description",
            version="1.4",
            param_df=self.param_df,
            info_df=self.info_df,
            coosys_df=self.coosys_df,
        )

        root = ET.fromstring(xml_str)
        ns = {"v": root.tag.split("}")[0].strip("{")}

        # Check description
        desc = root.find(".//v:DESCRIPTION", ns)
        self.assertIsNotNone(desc)
        self.assertEqual(desc.text, "Test Description")

        # Check fields
        fields = root.findall(".//v:FIELD", ns)
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0].attrib["name"], "ra")
        self.assertEqual(fields[1].attrib["name"], "dec")
        self.assertEqual(fields[0].attrib["unit"], "deg")
        self.assertEqual(fields[1].attrib["unit"], "deg")

        # Check param
        param = root.find(".//v:PARAM", ns)
        self.assertIsNotNone(param)
        self.assertEqual(param.attrib["name"], "creator")
        self.assertEqual(param.attrib["value"], "TestUser")

        # Check info
        info = root.find(".//v:INFO", ns)
        self.assertIsNotNone(info)
        self.assertEqual(info.attrib["name"], "version")
        self.assertEqual(info.attrib["value"], "1.0")

        # Check coosys
        coosys = root.find(".//v:COOSYS", ns)
        self.assertIsNotNone(coosys)
        self.assertEqual(coosys.attrib["ID"], "J2000")


if __name__ == "__main__":
    unittest.main()
