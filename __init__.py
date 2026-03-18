"""Importer-Package.

Stellt den generischen XML->SQLite-Importer als API bereit.
"""

from .importer import import_xml_blocks_to_sqlite

__all__ = ["import_xml_blocks_to_sqlite"]
