# # Insert rows into the NPI table
# INSERT_QUERY = """
#     INSERT INTO {table_name}
#     ({cols}, created_at, updated_at)
#     VALUES ({values}, now(), now())
#     ON DUPLICATE KEY UPDATE
#     {on_dupe_values}, updated_at=now()
# """

######################################
# RxVantageNDCMaster
######################################

CREATE_PRODUCT_MASTER_COMPLETE = """
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
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_PRODUCT_MASTER = """
    CREATE TABLE `{table_name}` (
    `id` int(11) NOT NULL DEFAULT 0,
    `client_product_id` varchar(50) DEFAULT NULL,
    `master_id` int(11) DEFAULT NULL,
    `master_type` varchar(20) DEFAULT NULL,
    `name` varchar(100) DEFAULT NULL,
    `generic_name` varchar(128) DEFAULT NULL,
    `drug_type` varchar(128) DEFAULT NULL,
    `source_type` varchar(128) DEFAULT NULL,
    `company_id` varchar(100) DEFAULT NULL,
    `is_admin_approved` varchar(128) DEFAULT NULL,
    `verified_source` varchar(55) DEFAULT NULL,
    `verified_source_id` varchar(45) DEFAULT NULL,
    `created_by` int(11) DEFAULT NULL,
    `created_date` date DEFAULT NULL,
    `modified_by` varchar(55) DEFAULT NULL,
    `modified_date` date DEFAULT NULL,
    `eff_date` date DEFAULT NULL,
    `end_date` date DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""
