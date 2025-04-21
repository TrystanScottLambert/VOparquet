"""
Testing the metadata module.
"""

import unittest
import pandas as pd
from metadata import ParquetMetaVO, get_names_and_datatypes


class TestParquetMetaVO(unittest.TestCase):
    """
    Main test class.
    """

    def setUp(self):
        self.fields = pd.DataFrame.from_dict(
            {
                "Name": ["RA", "DEC", "Z"],
                "Unit": ["deg", "deg", None],
                "Datatype": ["double", "double", "double"],
                "Description": ["Right Ascension", "Declination", "Redshift"],
                "UCD": ["pos.eq.ra", "pos.eq.dec", None],
                "ArraySize": [None, None, None],
            }
        )
        self.infos = [
            {"name": "SHARKv2-VERSION", "value": "2.1"},
            {"name": "Author", "value": "Trytan"},
        ]
        self.params = [
            {"name": "OmegaM", "value": 0.3, "datatype": "float", "unit": ""}
        ]
        self.description = "Test Table"

    def test_roundtrip_conversion(self):
        """
        Testing by converting to and from VOtable and ensuring that works.
        """
        original = ParquetMetaVO(
            fields=self.fields,
            params=self.params,
            infos=self.infos,
            description=self.description,
        )
        votable = original.to_votable()
        recovered = ParquetMetaVO.from_votable(votable)

        pd.testing.assert_frame_equal(
            original.fields.fillna(""), recovered.fields.fillna("")
        )
        self.assertEqual(original.description, recovered.description)
        self.assertEqual(original.params, recovered.params)
        self.assertEqual(original.infos, recovered.infos)

    def test_get_names_and_datatypes(self):
        """
        Testing that the data types are converted correctly.
        """
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "flag": [True, False, True],
                "value": [1.5, 2.3, 3.3],
                "name": ["a", "b", "c"],
            }
        )
        meta_df = get_names_and_datatypes(df)

        self.assertListEqual(list(meta_df.columns), ["Name", "Datatype"])
        self.assertSetEqual(set(meta_df["Name"]), {"id", "flag", "value", "name"})
        for dt in meta_df["Datatype"]:
            self.assertIn(
                dt, {"boolean", "int", "long", "float", "double", "unicodeChar"}
            )


if __name__ == "__main__":
    unittest.main()
