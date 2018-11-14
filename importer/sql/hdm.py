

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

CREATE_NDCMASTER_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `master_id` INT NOT NULL,
    `labelername` varchar(100) DEFAULT NULL,
    `productndc` varchar(20) DEFAULT NULL,
    `proprietaryname` varchar(60) DEFAULT NULL,
    `nonproprietaryname` varchar(255) DEFAULT NULL,
    `producttypename` varchar(35) DEFAULT NULL,
    `marketingcategoryname` varchar(70) DEFAULT NULL,
    `definition` varchar(100) DEFAULT NULL,
    `te_code` varchar(5) DEFAULT NULL,
    `type` varchar(20) DEFAULT NULL,
    `interpretation` varchar(75) DEFAULT NULL,
    `ndc_exclude_flag` char(1) DEFAULT NULL,
    `drug_id` varchar(10) DEFAULT NULL,
    `ind_drug_name` varchar(40) DEFAULT NULL,
    `ind_name` varchar(100) DEFAULT NULL,
    `status` varchar(20) DEFAULT NULL,
    `phase` varchar(20) DEFAULT NULL,
    `ind_detailedstatus` varchar(200) DEFAULT NULL,
    `eff_date` datetime DEFAULT NULL,
    `end_eff_date` datetime DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY (`productndc`, `ind_name`)
    )
"""
