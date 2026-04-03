# uofast-orm

Simple, fast ORM for U2 Unidata/UniVerse databases via [uopy](https://pypi.org/project/uopy/).

## Install

```bash
pip install uofast-orm
```

## Quick Start

```python
import uopy
from uofast_orm import UopyModel

class Customer(UopyModel):
    _file_name = "CUSTOMERS"
    _field_names = ["NAME", "EMAIL", "PHONE", "ADDRESS"]
    _field_map = {
        "full_name": "NAME",
        "email_address": "EMAIL",
        "phone_number": "PHONE",
        "street_address": "ADDRESS",
    }

with uopy.connect(host="...", user="...", password="...", account="...") as session:
    # Create
    c = Customer(session)
    c.full_name = "Jane Smith"
    c.email_address = "jane@example.com"
    c.create("CUST001")

    # Read
    c = Customer(session, record_id="CUST001")
    print(c.full_name)

    # Update
    c.phone_number = "555-1234"
    c.update()

    # Query
    results = c.select('NAME = "Jane Smith"')

    # Delete
    c.delete()
```

## Models

Subclass `UopyModel` and set class attributes:

| Attribute | Required | Description |
|-----------|----------|-------------|
| `_file_name` | Yes | U2 file name |
| `_field_names` | Yes* | List of DICT field names |
| `_field_map` | No | `{"python_name": "DICT_NAME"}` mapping |
| `_enable_cache` | No | Enable LRU cache (default: `False`) |
| `_cache_max_size` | No | Max cached records (default: `100`) |

\* Can be omitted when `_field_map` is provided.

## Named Fields with Alternative DICT

When multiple data files share a single DICT:

```python
from uofast_orm import SmartFile, patch_uopy_file

# Option A: monkey-patch uopy.File globally (zero call-site changes)
patch_uopy_file()

# Option B: use SmartFile directly
with SmartFile("ORDERS") as f:
    rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"],
                              dict_file="SHARED_DICT")
    f.write_named_fields("ORD001", {"AMOUNT": 999}, dict_file="SHARED_DICT")
```

## Code Generator

Auto-generate model classes from U2 DICT definitions (requires Ollama):

```bash
uofast-generate CUSTOMERS \
  --host 192.168.1.10 --user admin --password secret \
  --account /u2/MYACCOUNT \
  --output customer_model.py
```

Falls back to template-based generation if Ollama is unavailable.

## API Reference

### `UopyModel`

| Method | Description |
|--------|-------------|
| `load()` | Load record from database |
| `create(record_id)` | Create new record |
| `update()` | Update existing record |
| `delete()` | Delete record |
| `save()` | Create or update (auto-detects) |
| `get(field, default)` | Get field value (supports property or DB name) |
| `set(field, value)` | Set field value (supports property or DB name) |
| `to_dict(use_property_names)` | Export as dict |
| `select(stmt, limit, offset)` | Run SELECT statement, return model list |
| `read(record_id)` | Read single record, return model instance |
| `read_many(ids, batch_size)` | Batch read, return model list |
| `clear_cache()` | Clear LRU cache (classmethod) |

## Requirements

- Python 3.8+
- `uopy` (U2 Unidata/UniVerse client)
- `requests` (for code generator Ollama calls)

## License

MIT
