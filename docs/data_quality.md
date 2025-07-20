# Data Quality Checks and Monitoring

- Null checks on key columns.
- Duplicate detection and removal during deduplication steps.
- Validation of business logic:
  - Prices must be positive (> 0).
  - Quantities should be non-negative.
- Quality checks included before writes to silver and gold tables.
