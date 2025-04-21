"""
Testing the VOParquet module.
"""

import unittest
import os
import pandas as pd
from astropy.io.votable.tree import VOTableFile, Resource, TableElement, Field, Info
from vo_parquet.vo_parquet_table import VOParquetTable, read_vo_parquet_metadata


class TestVOParquetTable(unittest.TestCase):
    """Main test class"""
    def setUp(self):
        # Create test VOTable metadata
        self.votable = VOTableFile()
        resource = Resource()
        self.votable.resources.append(resource)
        table = TableElement(self.votable)
        resource.tables.append(table)
        table.fields.extend([
            Field(self.votable, name="ra", datatype="double", unit="deg"),
            Field(self.votable, name="dec", datatype="double", unit="deg"),
            Field(self.votable, name="z", datatype="double")
        ])
        table.description = "This is a test table"
        table.infos.extend([
            Info(name="SHARK-VERSION", value="SHARKv2"),
        ])

        # Create test data
        self.df = pd.DataFrame({
            "ra": [10.1, 20.2],
            "dec": [-30.1, -45.3],
            "z": [0.03, 0.1]
        })

        self.test_file = "test_vo_table.parquet"

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_write_and_read_parquet(self):
        """
        Testing that reading and writing is working correctly.
        """
        # Write to Parquet
        vo_table = VOParquetTable(self.df, self.votable)
        vo_table.write_to_parquet(self.test_file)

        # Read back
        reloaded = VOParquetTable.from_parquet(self.test_file)

        # Check data
        pd.testing.assert_frame_equal(reloaded.data, self.df)

        # Check metadata (field names and types)
        original_fields = self.votable.resources[0].tables[0].fields
        reloaded_fields = reloaded.meta_data.resources[0].tables[0].fields
        self.assertEqual(len(original_fields), len(reloaded_fields))
        for f1, f2 in zip(original_fields, reloaded_fields):
            self.assertEqual(f1.name, f2.name)
            self.assertEqual(f1.datatype, f2.datatype)
            self.assertEqual(f1.unit, f2.unit)

        # Check description
        self.assertEqual(
            self.votable.resources[0].tables[0].description,
            reloaded.meta_data.resources[0].tables[0].description,
        )

    def test_read_vo_parquet_metadata_direct(self):
        """
        Testing.
        """
        vo_table = VOParquetTable(self.df, self.votable)
        vo_table.write_to_parquet(self.test_file)

        meta = read_vo_parquet_metadata(self.test_file)
        table = meta.resources[0].tables[0]

        self.assertEqual(table.description, "This is a test table")
        self.assertEqual(len(table.fields), 3)
        self.assertEqual(table.fields[0].name, "ra")
        self.assertEqual(table.infos[0].name, "SHARK-VERSION")
        self.assertEqual(table.infos[0].value, "SHARKv2")


if __name__ == "__main__":
    unittest.main()
