
INSERT_AND_UPDATE_QUERY = """
    INSERT INTO {table_name}
    ({cols}, created_at, updated_at)
    VALUES ({values}, now(), now())
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}, updated_at=now()
"""

INSERT_QUERY = """
    INSERT INTO {table_name}
    ({cols}, created_at, updated_at)
    VALUES ({values}, now(), now())
"""

INSERT_PLAIN_Q = """
    INSERT INTO {table_name}
    ({cols})
    VALUES ({values})
"""

DELETE_Q = """
  DELETE from `{table_name}` 
  WHERE ( {where_clause} );
"""