"""
VOTable Class
"""

from dataclasses import dataclass
from typing import Optional
import warnings
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from read import extract_votable_metadata
from write import generate_votable_xml


@dataclass
class VOParquetTable:
    """
    Main data structure for storing data and metadata of VOparquet files.
    """

    data: pd.DataFrame
    metadata_field_df: Optional[pd.DataFrame] = None
    metadata_param_df: Optional[pd.DataFrame] = None
    metadata_info_df: Optional[pd.DataFrame] = None
    metadata_coosys_df: Optional[pd.DataFrame] = None
    raw_metadata: Optional[dict] = None
    description: Optional[str] = None
    version: str = "1.0"

    @classmethod
    def from_parquet(cls, filename: str) -> "VOParquetTable":
        """
        Creates a VOParquetTable object from the given file.
        """
        table = pq.read_table(filename)
        df = table.to_pandas()
        meta = table.schema.metadata

        if not meta or b"IVOA.VOTable-Parquet.content" not in meta:
            warnings.warn(
                f"File {filename} is not a valid VOParquet file. Metadata set to None."
            )
            return cls(data=df)

        votable_xml = meta[b"IVOA.VOTable-Parquet.content"].decode("utf-8")
        field_df, param_df, info_df, coosys_df, description = extract_votable_metadata(
            votable_xml
        )

        return cls(
            data=df,
            metadata_field_df=field_df,
            metadata_param_df=param_df,
            metadata_info_df=info_df,
            metadata_coosys_df=coosys_df,
            raw_metadata={k.decode(): v.decode() for k, v in meta.items()},
            description=description,
        )

    def write_to_parquet(self, filename: str, **kwargs) -> None:
        """
        Writes a parquet file with the given meta data.
        """
        table = pa.Table.from_pandas(self.data)
        metadata = dict(table.schema.metadata or {})

        votable_xml = generate_votable_xml(
            metadata_field_df=self.metadata_field_df,
            description=self.description,
            version=self.version,
            param_df=self.metadata_param_df,
            info_df=self.metadata_info_df,
            coosys_df=self.metadata_coosys_df,
        )

        metadata[b"IVOA.VOTable-Parquet.content"] = votable_xml.encode("utf-8")
        metadata[b"IVOA.VOTable-Parquet.version"] = self.version.encode("utf-8")

        schema_with_meta = table.schema.with_metadata(metadata)
        table = table.cast(schema_with_meta)
        pq.write_table(table, filename, **kwargs)
