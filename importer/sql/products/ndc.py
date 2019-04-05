######################################
# RxVantageNDCMaster
######################################

CREATE_NDCMASTER_DDL = """
    CREATE TABLE `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `master_id` INT DEFAULT NULL,
    `labelername` varchar(150) DEFAULT NULL,
    `productndc` varchar(20) DEFAULT NULL,
    `proprietaryname` varchar(60) DEFAULT NULL,
    `nonproprietaryname` varchar(255) DEFAULT NULL,
    `producttypename` varchar(35) DEFAULT NULL,
    `marketingcategoryname` varchar(70) DEFAULT NULL,
    `definition` varchar(100) DEFAULT NULL,
    `te_code` varchar(30) DEFAULT NULL,
    `te_type` varchar(20) DEFAULT NULL,
    `interpretation` varchar(75) DEFAULT NULL,
    `ndc_exclude_flag` char(1) DEFAULT NULL,
    `ind_drug_id` varchar(10) DEFAULT NULL,
    `ind_drug_name` varchar(40) DEFAULT NULL,
    `ind_name` varchar(100) DEFAULT NULL,
    `ind_status` varchar(20) DEFAULT NULL,
    `ind_phase` varchar(20) DEFAULT NULL,
    `ind_detailedstatus` varchar(200) DEFAULT NULL,
    `eff_date` datetime DEFAULT NULL,
    `end_eff_date` datetime DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_masterid` (`master_id`),
    KEY `idx_ndc_productndc_te_code_ind_name_ind_detailedstatus` (`productndc`,`te_code`,`ind_name`,`ind_detailedstatus`),
    KEY `idx_ndc_proprietaryname_nonproprietaryname` (`proprietaryname`,`nonproprietaryname`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""
