DO $$
DECLARE
    col_names text;
BEGIN
    -- Step 0: Add unique constraint if it doesn't exist
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = '{{temp_table_name}}_company_id_last_updated_at_key'
    ) THEN
        EXECUTE 'ALTER TABLE {{temp_table_name}} ADD CONSTRAINT {{temp_table_name}}_company_id_last_updated_at_key UNIQUE (company_id, last_updated_at)';
    END IF;

    -- Step 1: Create {{main_table}} if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{{main_table}}') THEN
        EXECUTE 'CREATE TABLE {{main_table}} (LIKE {{temp_table_name}} INCLUDING ALL)';
    END IF;

    -- Step 2: Get column names dynamically (excluding primary key columns)
    SELECT string_agg(column_name || ' = EXCLUDED.' || column_name, ', ')
    INTO col_names
    FROM information_schema.columns
    WHERE table_name = '{{temp_table_name}}'
      AND column_name NOT IN ('company_id', 'last_updated_at');

    -- Step 3: Perform upsert dynamically
    EXECUTE format(
        'INSERT INTO {{main_table}} SELECT * FROM {{temp_table_name}}
         ON CONFLICT (company_id, last_updated_at) DO UPDATE SET %s',
         col_names
    );
END $$;
