Usage

  # Phase 1 only — scan and generate the report
  .\Find-DuplicateMedia.ps1

  # Phase 1 + Phase 2 — scan, then prompt to delete
  .\Find-DuplicateMedia.ps1 -Delete

  # Dry-run — show what would be deleted, touch nothing
  .\Find-DuplicateMedia.ps1 -Delete -WhatIf

  # Delete using an existing report (skip re-scan)
  .\Find-DuplicateMedia.ps1 -Delete -ReportPath "D:\DuplicateReport.csv"

  ---
  How it works

  Phase 1 — Discovery

  Step: 1/4
  What happens: Get-ChildItem -Recurse walks D:\, skipping Program Files dirs and non-media extensions. Access errors
  are
    caught via -ErrorVariable and logged.
  ────────────────────────────────────────
  Step: 2/4
  What happens: Files are grouped by byte size. Only groups with ≥ 2 files need hashing (often cuts hashing work by
    90%+).
  ────────────────────────────────────────
  Step: 3/4
  What happens: SHA256 is computed for each candidate. Locked/inaccessible files are skipped and written to
    D:\ScanErrors.log.
  ────────────────────────────────────────
  Step: 4/4
  What happens: Within each hash group, the file with the shortest full path is marked ORIGINAL; all others DUPLICATE.
    Results go to D:\DuplicateReport.csv.

  Phase 2 — Deletion

  - Reads the in-memory results (or an existing CSV via -ReportPath)
  - Shows a preview of the first 10 targets and the total space recoverable
  - Requires you to type Y before anything is touched
  - Has two safety guards: skips files that no longer exist, and skips any DUPLICATE row that has no corresponding
  ORIGINAL in the report (protects against malformed CSVs)
  - Every outcome (DELETED / FAILED / SKIPPED) is appended to D:\DeletionLog.csv with a timestamp

  Output files

  ┌────────────────────────┬────────────────────────────────────────────────────────┐
  │          File          │                        Contents                        │
  ├────────────────────────┼────────────────────────────────────────────────────────┤
  │ D:\DuplicateReport.csv │ Hash, Status, FilePath, SizeBytes, SizeHuman, FileName │
  ├────────────────────────┼────────────────────────────────────────────────────────┤
  │ D:\DeletionLog.csv     │ Timestamp, FilePath, SizeBytes, Hash, Result, Error    │
  ├────────────────────────┼────────────────────────────────────────────────────────┤
  │ D:\ScanErrors.log      │ Timestamped warnings for locked/inaccessible files     │
  └────────────────────────┴────────────────────────────────────────────────────────┘
