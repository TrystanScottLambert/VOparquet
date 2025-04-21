"""
VOTable Class
"""

import io
from typing import ClassVar
from dataclasses import dataclass
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from astropy.io.votable.tree import VOTableFile
from astropy.io.votable import parse


@dataclass
class VOParquetTable:
    """
    Main data structure for storing data and metadata of VOparquet files.
    """
    data: pd.DataFrame
    meta_data: VOTableFile
    VERSION: ClassVar[str] = "1.0"

    @classmethod
    def from_parquet(cls, filename: str) -> "VOParquetTable":
        """
        Creates a VOParquetTable object from the given file.
        """
        meta_data = read_vo_parquet_metadata(filename)
        data_frame = pq.read_table(filename).to_pandas()
        return cls(data_frame, meta_data)

    def write_to_parquet(self, out_file: str) -> None:
        """
        Writes the VoParquetTable Object to a parquet file.
        """
        table = pa.Table.from_pandas(self.data)
        metadata = dict(table.schema.metadata or {})

        # Write VOTable object to XML string
        buffer = io.BytesIO()
        self.meta_data.to_xml(buffer)
        votable_xml = buffer.getvalue().decode("utf-8")

        metadata[b"IVOA.VOTable-Parquet.content"] = votable_xml.encode("utf-8")
        metadata[b"IVOA.VOTable-Parquet.version"] = self.VERSION.encode("utf-8")

        # Apply updated schema
        schema_with_meta = table.schema.with_metadata(metadata)
        table = table.cast(schema_with_meta)
        pq.write_table(table, out_file)

def read_vo_parquet_metadata(file_name: str) -> VOTableFile:
    """
    Reading the parquet meta data according to VOParquet standards.
    """
    meta = pq.read_metadata(file_name)
    xml_bytes = meta.metadata[b"IVOA.VOTable-Parquet.content"]
    buffer = io.BytesIO(xml_bytes)
    return parse(buffer)
