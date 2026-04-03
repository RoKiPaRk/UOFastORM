"""
named_fields.py
===============
Drop-in improvements for ``uopy.File.read_named_fields`` and
``uopy.File.write_named_fields`` that allow the caller to supply a *separate*
DICT file name instead of always deriving the dictionary from the data file.

Background
----------
The stock uopy implementation always opens ``DICT <file_name>`` to resolve
field positions.  This works for the common case but breaks (or produces
wrong results) when:

  * Multiple physical files share a single *canonical* DICT.
  * The data file lives in a different account/VOC path than its DICT.
  * You want to read/write fields using a layout defined in *another* file's
    DICT (cross-file column mapping, reporting overlays, etc.).
  * You are inside a tight loop and want to avoid re-opening the DICT on
    every iteration.

What changes vs. the original
------------------------------
1.  ``read_named_fields(id, field_names, *, dict_file=None)``
    ``write_named_fields(id, field_dict, *, dict_file=None)``

    Both methods gain an optional *keyword-only* ``dict_file`` argument:

      - ``None`` (default)   -> original behaviour; opens ``DICT <file_name>``
      - ``str``              -> name of an alternative DICT file to open
      - ``uopy.File``        -> already-open File object (caller owns lifecycle;
                               no extra open/close per call -- ideal for loops)

2.  DICT descriptor parsing is more robust:
      - D-type  (data) descriptors     -> attribute 2 is the field number
      - A/S-type (synonym) descriptors -> attribute 2 is the field number
      - I/V/C-type (virtual)           -> silently skipped on write,
                                          returned as None on read
      - Missing / malformed DICT items -> None / silently skipped

3.  ``NamedFieldsMixin``  -- inject the improvements into any uopy.File
    subclass via normal Python inheritance.

4.  ``SmartFile``         -- a ready-to-use uopy.File subclass.

5.  ``patch_uopy_file()`` -- monkey-patches uopy.File at runtime for code
    bases that cannot change their call sites.

Usage
-----

Option A - monkey-patch (zero call-site changes)::

    import uopy
    from uofast_orm.named_fields import patch_uopy_file
    patch_uopy_file()

    with uopy.connect(...) as s:
        f = uopy.File("ORDERS")

        # default: still uses DICT ORDERS
        rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"])

        # alternative DICT opened per-call
        rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"],
                                  dict_file="SHARED_DICT")

        # pre-opened DICT -- most efficient inside a loop
        with uopy.File("SHARED_DICT") as d:
            for oid in order_ids:
                rec = f.read_named_fields(oid, ["CUSTNO", "AMOUNT"],
                                          dict_file=d)
                rec["AMOUNT"] = recalculate(rec["AMOUNT"])
                f.write_named_fields(oid, rec, dict_file=d)


Option B - subclass::

    from uofast_orm.named_fields import NamedFieldsMixin
    import uopy

    class SmartFile(NamedFieldsMixin, uopy.File):
        pass

    with uopy.connect(...) as s:
        f = SmartFile("ORDERS")
        rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"],
                                  dict_file="CANONICAL_DICT")


Option C - use SmartFile directly::

    from uofast_orm.named_fields import SmartFile
    import uopy

    with uopy.connect(...) as s:
        f = SmartFile("ORDERS")
        rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"],
                                  dict_file="CANONICAL_DICT")
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Generator, Iterable, Optional, Union

import uopy  # must already be installed / connected

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------
_DictFileArg = Optional[Union[str, "uopy.File"]]


# ---------------------------------------------------------------------------
# DICT descriptor type sets
# ---------------------------------------------------------------------------

# Types that carry a fixed storage field number in attribute 2
_POSITIONAL_TYPES = frozenset({"D", "A", "S", "DA", "SA"})

# Types that are computed/virtual -- no storage position
_VIRTUAL_TYPES = frozenset({"I", "V", "C"})


# ---------------------------------------------------------------------------
# Internal: DICT file context manager
# ---------------------------------------------------------------------------

@contextmanager
def _open_dict(
    dict_file: _DictFileArg,
    data_file_name: str,
) -> Generator["uopy.File", None, None]:
    """
    Yield an open ``uopy.File`` for the DICT, then clean up.

    Ownership rules:
      * dict_file is None     -> open "DICT <data_file_name>", close on exit
      * dict_file is a str    -> open that name as a File, close on exit
      * dict_file is a File   -> yield as-is; caller owns the lifecycle
    """
    if isinstance(dict_file, uopy.File):
        yield dict_file
        return

    name = f"DICT {data_file_name}" if dict_file is None else dict_file
    f = uopy.File(name)
    try:
        yield f
    finally:
        f.close()


# ---------------------------------------------------------------------------
# Internal: DICT item resolution
# ---------------------------------------------------------------------------

def _resolve_field_position(dict_f: "uopy.File", field_name: str) -> int:
    """
    Read the DICT item for *field_name* and return its 1-based field number.

    Return values
    -------------
    > 0   valid storage position (1-based attribute number)
    -1    virtual/computed field (I, V, or C type) -- no storage position
     0    unknown, missing, or malformed DICT item
    """
    try:
        record = dict_f.read(field_name)
    except Exception:
        return 0

    # Attribute 1 (index 0) = descriptor type
    dtype = str(record[0]).strip().upper() if len(record) > 0 and record[0] else ""

    if dtype in _VIRTUAL_TYPES:
        return -1

    if dtype in _POSITIONAL_TYPES or dtype == "":
        # Empty type -> assume D-type (common in legacy accounts)
        raw = record[1] if len(record) > 1 else ""
        try:
            pos = int(str(raw).strip())
            return pos if pos > 0 else 0
        except (ValueError, TypeError):
            return 0

    return 0  # unrecognised type


# ---------------------------------------------------------------------------
# Core public implementations
# ---------------------------------------------------------------------------

def read_named_fields(
    self: "uopy.File",
    record_id: str,
    field_names: Iterable[str],
    *,
    dict_file: _DictFileArg = None,
) -> Dict[str, object]:
    """
    Read a record and return the requested fields as a {name: value} dict.

    Parameters
    ----------
    record_id:
        The record key to read.
    field_names:
        Iterable of DICT field/column names to retrieve.
    dict_file:
        Controls which DICT file is used to resolve field positions:

        None (default)
            Open ``DICT <this_file_name>`` -- identical to original uopy.
        str
            Name of an alternative DICT file to open for this call.
        uopy.File
            An already-open File object.  The caller is responsible for
            opening and closing it; most efficient inside a loop.

    Returns
    -------
    dict
        {field_name: value} for every name in field_names.

        * Virtual (I/V/C) fields -> None.
        * Fields not found in the DICT -> None.
        * If the data record does not exist -> all values are None.
    """
    field_names = list(field_names)

    try:
        record = self.read(record_id)
    except uopy.UOError:
        return {name: None for name in field_names}

    result: Dict[str, object] = {}
    data_file_name: str = self.name

    with _open_dict(dict_file, data_file_name) as dict_f:
        for name in field_names:
            pos = _resolve_field_position(dict_f, name)
            if pos > 0:
                idx = pos - 1  # uopy DynArray is 0-indexed
                try:
                    result[name] = record[idx]
                except IndexError:
                    result[name] = None
            else:
                result[name] = None  # unknown (0) or virtual (-1)

    return result


def write_named_fields(
    self: "uopy.File",
    record_id: str,
    field_dict: Dict[str, object],
    *,
    dict_file: _DictFileArg = None,
) -> None:
    """
    Write specific named fields into a record (read-modify-write).

    Parameters
    ----------
    record_id:
        The record key to update.
    field_dict:
        {field_name: new_value} mapping of fields to set.
    dict_file:
        Controls which DICT file is used to resolve field positions.
        Same semantics as read_named_fields.

    Notes
    -----
    * Attributes not present in field_dict are left untouched.
    * If the record does not yet exist it is created as a new empty record.
    * Virtual (I/V/C) DICT items are silently skipped -- no storage position.
    * Fields not found in the DICT are silently skipped.
    * The write() call is only issued if at least one field was resolved and
      updated, avoiding a spurious write when all names are unknown/virtual.
    """
    if not field_dict:
        return

    data_file_name: str = self.name

    try:
        record = self.read(record_id)
    except uopy.UOError:
        record = uopy.DynArray()

    wrote_anything = False

    with _open_dict(dict_file, data_file_name) as dict_f:
        for name, value in field_dict.items():
            pos = _resolve_field_position(dict_f, name)
            if pos <= 0:
                # 0 = unknown, -1 = virtual -- skip both
                continue
            idx = pos - 1  # 0-based
            while len(record) <= idx:
                record.append("")
            record[idx] = value
            wrote_anything = True

    if wrote_anything:
        self.write(record_id, record)


# ---------------------------------------------------------------------------
# Mixin
# ---------------------------------------------------------------------------

class NamedFieldsMixin:
    """
    Mixin that injects improved read_named_fields / write_named_fields into
    any uopy.File subclass.

    Put this mixin *before* uopy.File in the MRO so Python resolves these
    methods here first::

        class SmartFile(NamedFieldsMixin, uopy.File):
            pass
    """

    def read_named_fields(
        self,
        record_id: str,
        field_names: Iterable[str],
        *,
        dict_file: _DictFileArg = None,
    ) -> Dict[str, object]:
        """See module-level read_named_fields for full documentation."""
        return read_named_fields(self, record_id, field_names,
                                 dict_file=dict_file)

    def write_named_fields(
        self,
        record_id: str,
        field_dict: Dict[str, object],
        *,
        dict_file: _DictFileArg = None,
    ) -> None:
        """See module-level write_named_fields for full documentation."""
        write_named_fields(self, record_id, field_dict, dict_file=dict_file)


# ---------------------------------------------------------------------------
# Ready-to-use subclass
# ---------------------------------------------------------------------------

class SmartFile(NamedFieldsMixin, uopy.File):
    """
    uopy.File subclass with the improved named-field methods built in.

    Drop-in replacement for uopy.File wherever the dict_file parameter
    is needed::

        from uofast_orm.named_fields import SmartFile
        import uopy

        with uopy.connect(...) as s:
            f = SmartFile("ORDERS")
            rec = f.read_named_fields("ORD001", ["CUSTNO", "AMOUNT"],
                                      dict_file="CANONICAL_DICT")
            rec["AMOUNT"] = 999
            f.write_named_fields("ORD001", rec, dict_file="CANONICAL_DICT")
    """


# ---------------------------------------------------------------------------
# Monkey-patch helper
# ---------------------------------------------------------------------------

def patch_uopy_file() -> None:
    """
    Replace uopy.File.read_named_fields and uopy.File.write_named_fields
    with the improved versions at runtime.

    Call this once at application startup, after ``import uopy``.
    All uopy.File instances will automatically gain the dict_file parameter.

    Example::

        import uopy
        from uofast_orm.named_fields import patch_uopy_file
        patch_uopy_file()
    """

    def _read(self, record_id, field_names, *, dict_file=None):
        return read_named_fields(self, record_id, field_names,
                                 dict_file=dict_file)

    def _write(self, record_id, field_dict, *, dict_file=None):
        write_named_fields(self, record_id, field_dict, dict_file=dict_file)

    _read.__name__ = "read_named_fields"
    _read.__qualname__ = "File.read_named_fields"
    _read.__doc__ = read_named_fields.__doc__

    _write.__name__ = "write_named_fields"
    _write.__qualname__ = "File.write_named_fields"
    _write.__doc__ = write_named_fields.__doc__

    uopy.File.read_named_fields = _read    # type: ignore[method-assign]
    uopy.File.write_named_fields = _write  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------
__all__ = [
    "read_named_fields",
    "write_named_fields",
    "NamedFieldsMixin",
    "SmartFile",
    "patch_uopy_file",
]
