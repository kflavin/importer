# Insert rows into the NPI table
INSERT_QUERY = """
    INSERT INTO {table_name}
    ({cols}, created_at, updated_at)
    VALUES ({values}, now(), now())
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}, updated_at=now()
"""