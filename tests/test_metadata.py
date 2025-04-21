"""
Main testing unit for metadata module.
"""

import unittest
import pandas as pd
import numpy as np
from astropy.io.votable.tree import VOTableFile, Resource, TableElement, Field, Param, Info
from vo_parquet.metadata import ParquetMetaVO, get_names_and_datatypes, dtype_to_vodatatype

class TestParquetMetaVO(unittest.TestCase):
    def setUp(self):
        # Prepare a fields DataFrame with all possible columns
        self.fields = pd.DataFrame({
            "Name": ["RA", "DEC", "Z"],
            "Datatype": ["double", "double", "double"],
            "UCD": ["pos.eq.ra", "pos.eq.dec", None],
            "Unit": ["deg", "deg", None],
            "ArraySize": [None, None, None],
            "Description": ["Right Ascension", "Declination", "Redshift"]
        })
        # Params include name, value, datatype, and unit
        self.params = [
            {"name": "OmegaM", "value": 0.3, "datatype": "float", "unit": ""}
        ]
        # Infos include only name and value
        self.infos = [
            {"name": "SHARKv2-VERSION", "value": "2.1"},
            {"name": "Author", "value": "Trytan"}
        ]
        self.description = "Test Table"

    def test_roundtrip_conversion(self):
        original = ParquetMetaVO(
            fields=self.fields,
            params=self.params,
            infos=self.infos,
            description=self.description
        )
        votable = original.to_votable()
        recovered = ParquetMetaVO.from_votable(votable)

        # Compare fields DataFrame ignoring column order and NaNs
        expected = original.fields.sort_index(axis=1).fillna("")
        actual = recovered.fields.sort_index(axis=1).fillna("")
        pd.testing.assert_frame_equal(expected, actual)

        # Description round-trip
        self.assertEqual(recovered.description, self.description)

        # Params round-trip should preserve all keys
        self.assertEqual(len(recovered.params), len(self.params))
        self.assertDictEqual(recovered.params[0], self.params[0])

        # Infos round-trip: only name and value guaranteed
        got_infos = {(info["name"], info["value"]) for info in recovered.infos}
        want_infos = {(d["name"], d["value"]) for d in self.infos}
        self.assertSetEqual(got_infos, want_infos)

    def test_empty_metadata(self):
        # No params, no infos
        fields = pd.DataFrame({"Name": [], "Datatype": []})
        meta = ParquetMetaVO(fields=fields)
        votable = meta.to_votable()
        recovered = ParquetMetaVO.from_votable(votable)

        # Fields empty
        self.assertTrue(recovered.fields.empty)
        # Params and infos should be None
        self.assertIsNone(recovered.params)
        self.assertIsNone(recovered.infos)
        # Description None
        self.assertIsNone(recovered.description)

    def test_missing_param_keys_raises(self):
        bad = [{"value": 1.0}]
        with self.assertRaises(ValueError):
            ParquetMetaVO(fields=self.fields, params=bad).to_votable()

    def test_missing_info_keys_raises(self):
        bad = [{"name": "X"}]
        with self.assertRaises(ValueError):
            ParquetMetaVO(fields=self.fields, infos=bad).to_votable()

class TestHelpers(unittest.TestCase):
    def test_dtype_to_vodatatype_all_types(self):
        # Boolean
        self.assertEqual(dtype_to_vodatatype(np.dtype(bool)), "boolean")
        # Integers
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.uint8)), "unsignedByte")
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.int16)), "short")
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.int32)), "int")
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.int64)), "long")
        # Floats
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.float32)), "float")
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.float64)), "double")
        # Complex
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.complex64)), "floatComplex")
        self.assertEqual(dtype_to_vodatatype(np.dtype(np.complex128)), "doubleComplex")
        # String (unicode)
        self.assertEqual(dtype_to_vodatatype(np.dtype("U5")), "unicodeChar")
        # Object fallback
        self.assertEqual(dtype_to_vodatatype(np.dtype(object)), "unicodeChar")
        # Unsupported
        with self.assertRaises(ValueError):
            dtype_to_vodatatype(np.dtype("datetime64[ns]"))

    def test_get_names_and_datatypes(self):
        df = pd.DataFrame({
            "a": [1, 2],
            "b": [True, False],
            "c": [1.5, 2.5],
            "d": ["x", "y"]
        })
        meta = get_names_and_datatypes(df)
        # Expect same column order and correct mappings
        self.assertListEqual(list(meta["Name"]), ["a", "b", "c", "d"])
        self.assertListEqual(
            list(meta["Datatype"]),
            ["long", "boolean", "double", "unicodeChar"]
        )

if __name__ == "__main__":
    unittest.main()
