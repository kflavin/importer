# Insert rows into the NPI table
INSERT_QUERY = """
    INSERT INTO {table_name}
    ({cols}, created_at, updated_at)
    VALUES ({values}, now(), now())
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}, updated_at=now()
"""

######################################
# RxVantageNDCMaster
######################################

CREATE_PRODUCTMASTER_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `master_id` INT DEFAULT NULL,
    `master_type` VARCHAR(128) DEFAULT NULL,
    `proprietaryname` VARCHAR(512) DEFAULT NULL,
    `nonproprietaryname` VARCHAR(512) DEFAULT NULL,
    `eff_date` DATE DEFAULT NULL,
    `end_eff_date` DATE DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
    );
"""
