SELECT_Q = """
    SELECT * from {table_name}
"""

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

COPY_TABLE_DDL = """
    CREATE TABLE `{new_table_name}` LIKE `{old_table_name}`
"""

COPY_TABLE_DATA_DML = """
    INSERT `{new_table_name}` SELECT * FROM `{old_table_name}`
"""

CREATE_TABLE_DDL = """
    CREATE TABLE `{table_name}`
"""

DROP_TABLE_DDL = """
   DROP TABLE `{table_name}`
"""

DROP_TABLE_IFE_DDL = """
   DROP TABLE IF EXISTS `{table_name}`
"""

RENAME_TABLE_DDL = """
   RENAME TABLE `{old_table_name}` TO `{new_table_name}`
"""

CREATE_TABLE_LIKE_DDL = """
    CREATE TABLE {target_table_name} LIKE {source_table_name};
"""

CREATE_TABLE_LIKE_IFNE_DDL = """
    CREATE TABLE IF NOT EXISTS {target_table_name} LIKE {source_table_name};
"""

TRUNCATE_TABLE_DML = """
    TRUNCATE TABLE {table_name};
"""