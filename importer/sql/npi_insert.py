INSERT_WEEKLY_QUERY = """
    INSERT INTO {table_name}
    ({cols})
    VALUES ({values})
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}
"""

INSERT_MONTHLY_QUERY = """
    set global net_buffer_length=1000000;
    set global max_allowed_packet=1000000000;
    SET foreign_key_checks = 0;
    SET UNIQUE_CHECKS = 0;
    SET AUTOCOMMIT = 0;

    LOAD DATA LOCAL INFILE '{infile}' INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;

    SET foreign_key_checks = 1;
    SET UNIQUE_CHECKS = 1;
    SET AUTOCOMMIT = 1;
"""
