INSERT_WEEKLY_QUERY = """
    INSERT INTO {table_name}
    ({cols})
    VALUES ({values})
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}
"""

INSERT_MONTHLY_QUERY = """
    LOAD DATA LOCAL INFILE '{infile}' INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
"""
