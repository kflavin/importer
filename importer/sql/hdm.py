

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

CREATE_NDC_MASTER_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `master_id` INT NOT NULL,
    `labelername` varchar(121) DEFAULT NULL,
    `productndc` varchar(10) DEFAULT NULL,
    `proprietaryname` varchar(226) DEFAULT NULL,
    `nonproprietaryname` varchar(512) DEFAULT NULL,
    `producttypename` varchar(27) DEFAULT NULL,
    `marketingcategoryname` varchar(40) DEFAULT NULL,
    `definition` varchar(100) DEFAULT NULL,
    `te_code` varchar(50) DEFAULT NULL,
    `type` varchar(50) DEFAULT NULL,
    `interpretation` varchar(255) DEFAULT NULL,
    `ndc_exclude_flag` varchar(1) DEFAULT NULL,
    `drug_id` varchar(8) DEFAULT NULL,
    `ind_drug_name` varchar(50) DEFAULT NULL,
    `ind_name` varchar(255) DEFAULT NULL,
    `status` varchar(10) DEFAULT NULL,
    `phase` varchar(10) DEFAULT NULL,
    `ind_detailedstatus` varchar(30) DEFAULT NULL,
    `eff_date` datetime DEFAULT NULL,
    `end_eff_date` datetime DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    -- UNIQUE KEY (`productndc`, `drug_id`, `ind_name`, `ind_drug_name`, `status`, `phase`, `ind_detailedstatus`)
    )
"""
