"""Batch-Startpunkt fuer mehrere XML-Importjobs.

Hinweis:
- Jeder Job nutzt den generischen Importer aus `importers.importer`.
- PK/FK-Entscheidungen werden weiterhin interaktiv abgefragt.
"""

from dataclasses import dataclass
from pathlib import Path

from importers import import_xml_blocks_to_sqlite


@dataclass(frozen=True)
class ImportJob:
    """Konfiguration fuer einen einzelnen XML-Importlauf."""

    xml_path: Path
    xsd_main: Path
    xsd_defs: Path
    sqlite_path: Path
    element_name: str
    complex_type_name: str | None = None


BASE = Path(
    r"BVNG_Advanced_A1_Var1_Ed_01_120\BVNG_Advanced_A1_Var1_Ed_01_120\STBD_Outside_(Shaft1)_SPU_SW_Image_26.02.2025\Node_1"
)

JOBS = [
    ImportJob(
        xml_path=BASE / "project_data" / "CANBus.xml",
        xsd_main=Path(r"stylesheets\schemas\CANBus.xsd"),
        xsd_defs=Path(r"stylesheets\schemas\MCS6_Definitions.xsd"),
        sqlite_path=Path("out.db"),
        element_name="Bus",
        complex_type_name="Bus_type",
    ),
]


def main() -> None:
    """Fuehrt alle konfigurierten Jobs nacheinander aus."""
    for idx, job in enumerate(JOBS, start=1):
        print(f"\n=== Job {idx}/{len(JOBS)}: {job.xml_path.name} ===")
        import_xml_blocks_to_sqlite(
            xml_path=job.xml_path,
            xsd_main=job.xsd_main,
            xsd_defs=job.xsd_defs,
            sqlite_path=job.sqlite_path,
            element_name=job.element_name,
            complex_type_name=job.complex_type_name,
        )


if __name__ == "__main__":
    main()
