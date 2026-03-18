# XML -> SQLite Importer

## Projektstruktur
- `importers/importer.py`: generischer Importer mit XSD-Typauflosung und rekursivem Nested-Import.
- `importers/__init__.py`: einfacher API-Export (`import_xml_blocks_to_sqlite`).
- `script.py`: Batch-Einstieg fuer definierte Importjobs.
- `db_abfrage.py`: kleines Helferskript zum schnellen Lesen aus `out.db`.

## Wichtige Eigenschaften des Importers
- erzeugt die Haupttabelle aus einem XML-Block (z. B. `Bus`).
- erkennt verschachtelte Child-Elemente und legt dafuer eigene Tabellen an (dedupliziert ueber PK).
- fragt PK/FK interaktiv ab.
- erstellt zwischen jeder Ebene Link-Tabellen mit FK auf Parent und Child (m:n-faehig).
- behandelt PK-Duplikate robust: Datensatz wird uebersprungen, Import laeuft weiter.
- kann alle relevanten XML-Dateien in `project_data` automatisch durchlaufen.
- speichert Key-Auswahlen in `importers/key_selection.json` und nutzt sie im Update-Modus ohne Rueckfragen.

## Start
```powershell
.\.venv\Scripts\python.exe importers\importer.py
```
Beim Start waehlt man:
- `Einzelne XML waehlen und Keys festlegen`
- `Dateiliste fuer automatisierten DB-Update festlegen`
- `Automatisierten DB-Update ausfuehren`

Hinweis:
- Eine XML kann jederzeit spaeter erneut ausgewaehlt und neu konfiguriert werden.
- Die Dateiliste fuer den automatisierten Update-Lauf wird persistent gespeichert und kann jederzeit neu gesetzt werden.
- Wenn keine passende XSD automatisch gefunden wird, kann die XSD manuell ausgewaehlt werden. Diese Zuordnung wird gespeichert.
- Gleiches gilt, wenn zwar eine XSD gefunden wird, aber daraus keine Importziele erkannt werden.

## Batch-Start
```powershell
.\.venv\Scripts\python.exe script.py
```

## Hinweise
- Der grosse Datenordner `BVNG_Advanced_A1_Var1_Ed_01_120` wird nicht veraendert.
- Temp-/Cache-Dateien sind in `.gitignore` eingetragen.
