"""
uofast-orm — Simple ORM for U2 Unidata databases
=================================================
"""

from .core import UopyModel
from .named_fields import NamedFieldsMixin, SmartFile, patch_uopy_file
from .generator import ORMClassGenerator

__all__ = [
    "UopyModel",
    "NamedFieldsMixin",
    "SmartFile",
    "patch_uopy_file",
    "ORMClassGenerator",
]
__version__ = "1.1.0"
