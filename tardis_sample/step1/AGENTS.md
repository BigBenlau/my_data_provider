# Step 1 report editing workflow

- Treat the current `step1_report.md` as the authoritative source, including all user edits and deletions.
- Before every report change, read the latest Chinese report. Edit it first, then align `step1_report_en.md` with its current content, structure, tables, numbers, links, and cross-references.
- Do not restore deleted material from an older report, the empirical results, or `render_step1_reports.py` unless explicitly requested.
- Do not run the historical renderer to overwrite the delivery reports: its template does not incorporate the user's subsequent edits.
- Keep the English report fully translated, while preserving API identifiers, numbers, URLs, and original English JSON descriptions.
- Do not mention sample files, analysis scripts, internal result paths, or completion status in the formal reports.
