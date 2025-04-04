# surfe

## Design decisions
- Detect and process **new/unprocessed files**
  - New files are added to the bucket on a daily basis.
  - 
- Transform JSON → Parquet
  - Save transformed data:
      - Back to **S3** (same naming/partitioning convention)
        - Into **Postgres** database
          - Bulk Upserts
            - Advantages of Upserts
                - Efficiently handles both insertions and updates in one operation.
                - Ensures data integrity by resolving conflicts based on constraints.



{"company_id":3299799,"source_id":"5398596","company_name":"GND Partners","company_name_alias"