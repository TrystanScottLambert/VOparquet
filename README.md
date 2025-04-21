# VOparquet

**VOparquet** is a Python package for working with **Virtual Observatory (VO) metadata** and **Parquet-based tabular data**. It enables you to generate, manipulate, and read **VO-compliant metadata** for astronomical data stored in efficient [Apache Parquet](https://parquet.apache.org/) formats.

----------

## 📦 Installation

```bash
pip install VOparquet

```

----------

## 🚀 Core Functionality

VOparquet’s core revolves around two components:

1.  A `pandas.DataFrame` holding the actual tabular data.
    
2.  An `astropy.io.votable.tree.VOTableFile` representing the VO metadata.
    

This design gives users maximum flexibility: you can construct any valid VOTable metadata object (as defined in the [VOParquet specification](https://www.ivoa.net/documents/Notes/VOParquet/20250116/index.html)). By leveraging `astropy`, you also gain access to a rich ecosystem of VOTable utilities.

----------

## 🛠 Building Metadata from Scratch

Using `astropy` lets you fully leverage the VOTable format’s flexibility—defining custom `FIELD`s, `PARAM`s, `INFO` elements, units, UCDs, and more.

For example, reproduce the metadata-only table from the VOParquet documentation:

### Example

```python
from astropy.io.votable.tree import VOTableFile, Resource, Table, Field, Param
from astropy.io.votable import writeto

# Create VOTable structure
votable = VOTableFile(version="1.4")
resource = Resource()
votable.resources.append(resource)

# Create TABLE element
table = Table(votable)
table.name = "MessierObjects"
table.description = "Nebulae and clusters"

# Add PARAM
param = Param(votable, name="author", datatype="char", arraysize="*", value="Charles Messier")
table.params.append(param)

# Add FIELDs
field_id = Field(votable, name="ID", datatype="long")
field_id.description = "Source identifier"

field_ra = Field(votable, name="RA", datatype="double", unit="deg", ucd="pos.eq.ra")
field_ra.description = "ICRS Right Ascension"

field_dec = Field(votable, name="DEC", datatype="double", unit="deg", ucd="pos.eq.dec")
field_dec.description = "ICRS Declination"

table.fields.extend([field_id, field_ra, field_dec])

# Add table to resource
resource.tables.append(table)

# (Optional) Save to file
writeto(votable, "messier_metadata.vot")

```

You can then build the Parquet file using `VOParquetTable`:

```python
from vo_parquet.vo_parquet_table import VOParquetTable
import pandas as pd

df = pd.DataFrame({
    "ID": [1, 2, 3],
    "RA": [10.684, 83.822, 201.365],   # in degrees
    "DEC": [41.269, -5.391, -47.479]   # in degrees
})

vp = VOParquetTable(df, votable)

```

----------

## 🛠 Helper Functions

When you only need basic metadata, manually building the full `astropy` structure can be verbose. The `metadata` module offers two helpers:

-   `get_names_and_datatypes(df)`: Creates a DataFrame with `Name` and `Datatype` columns from your data.
    
-   `ParquetMetaVO`: A class to build or parse VOTable metadata more succinctly.
    

### Creating from an existing VOTable

```python
from vo_parquet.metadata import ParquetMetaVO

vpt = ParquetMetaVO.from_votable(votable)

```

### Building from scratch

```python
from vo_parquet.metadata import get_names_and_datatypes, ParquetMetaVO

# Generate fields DataFrame
field_df = get_names_and_datatypes(df)
field_df["description"] = ["Source identifier", "ICRS Right Ascension", "ICRS Declination"]
field_df["unit"] = ["", "deg", "deg"]
field_df["ucd"] = ["", "pos.eq.ra", "pos.eq.dec"]

# Define PARAMs (and optionally INFO)
params = [{"name": "author", "datatype": "char", "value": "Charles Messier"}]

vpt = ParquetMetaVO(field_df, params, description="Nebulae and clusters")

# Convert to VOTableFile and integrate
vo_table = vpt.to_votable()
vp = VOParquetTable(df, vo_table)

```

This approach is more compact and leverages DataFrame operations for customization. You can also include `INFO` metadata by passing a list of info dictionaries to `ParquetMetaVO`.

----------

## 📖 Reading Parquet + VO Metadata

Load any Parquet file; if VO metadata is present, it’s parsed automatically:

```python
from vo_parquet.vo_parquet_table import VOParquetTable

vp = VOParquetTable.from_parquet("test.parquet")
data = vp.data        # pandas DataFrame
meta = vp.meta_data   # astropy VOTableFile (or None)

```

----------

## 💾 Writing Parquet + VO Metadata

```python
vp.write_to_parquet("test.parquet")

```