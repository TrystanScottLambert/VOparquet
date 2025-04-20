"""
Testing the vo_parquet module.
"""

import os
import unittest
import pandas as pd
from vo_parquet import VOParquetTable


class TestVOParquetTable(unittest.TestCase):
    """
    Testing the main class for VO Parquet tables.
    """

    def setUp(self):
        self.data = pd.DataFrame(
            {"ra": [10.1, 20.2], "dec": [-30.1, -45.3], "z": [0.03, 0.1]}
        )

        self.metadata_field_df = pd.DataFrame(
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
                {
                    "Name": "z",
                    "Unit": "",
                    "UCD": "src.redshift",
                    "Datatype": "double",
                    "Arraysize": "1",
                    "Description": "Redshift",
                },
            ]
        )

        self.metadata_param_df = pd.DataFrame(
            [{"name": "creator", "value": "TestAuthor"}]
        )

        self.metadata_info_df = pd.DataFrame(
            [{"name": "pipeline_version", "value": "v1.0"}]
        )

        self.metadata_coosys_df = pd.DataFrame(
            [{"ID": "J2000", "equinox": "2000.0", "epoch": "2000", "system": "eq_FK5"}]
        )

        self.temp_file = "test_voparquet.parquet"

    def test_write_and_read_voparquet(self):
        """
        Main test functionality.
        """
        vp = VOParquetTable(
            data=self.data,
            metadata_field_df=self.metadata_field_df,
            metadata_param_df=self.metadata_param_df,
            metadata_info_df=self.metadata_info_df,
            metadata_coosys_df=self.metadata_coosys_df,
            description="Test VOParquet",
        )

        # Write to file
        vp.write_to_parquet(self.temp_file)

        # Read back in
        vp2 = VOParquetTable.from_parquet(self.temp_file)

        # Compare data
        pd.testing.assert_frame_equal(vp2.data, self.data)
        pd.testing.assert_frame_equal(vp2.metadata_field_df, self.metadata_field_df)
        pd.testing.assert_frame_equal(vp2.metadata_param_df, self.metadata_param_df)
        pd.testing.assert_frame_equal(vp2.metadata_info_df, self.metadata_info_df)
        pd.testing.assert_frame_equal(vp2.metadata_coosys_df, self.metadata_coosys_df)
        self.assertEqual(vp2.description, "Test VOParquet")

    def tearDown(self):
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)


if __name__ == "__main__":
    unittest.main()
