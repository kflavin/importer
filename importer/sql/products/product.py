######################################
# RxVantageNDCMaster
######################################

# # Old
# CREATE_PRODUCT_MASTER_DDL = """
#     CREATE TABLE IF NOT EXISTS `{table_name}` (
#     `id` INT NOT NULL AUTO_INCREMENT,
#     `master_id` INT DEFAULT NULL,
#     `master_type` VARCHAR(128) DEFAULT NULL,
#     `proprietaryname` VARCHAR(512) DEFAULT NULL,
#     `nonproprietaryname` VARCHAR(512) DEFAULT NULL,
#     `eff_date` DATE DEFAULT NULL,
#     `end_eff_date` DATE DEFAULT NULL,
#     `created_at` datetime DEFAULT NULL,
#     `updated_at` datetime DEFAULT NULL,
#     PRIMARY KEY (`id`)
#     ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
# """

CREATE_PRODUCT_MASTER_DDL = """
    CREATE TABLE `{table_name}` (
    `master_id` INT NOT NULL AUTO_INCREMENT,
    `master_type` VARCHAR(128) DEFAULT NULL,
    `proprietaryname` VARCHAR(512) DEFAULT NULL,
    `nonproprietaryname` VARCHAR(512) DEFAULT NULL,
    `eff_date` DATE DEFAULT NULL,
    `end_eff_date` DATE DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`master_id`),
    KEY `idx_master_id_master_type` (`master_id`,`master_type`),
    KEY `idx_proprietaryname_nonproprietaryname` (`proprietaryname`(255),`nonproprietaryname`(255))
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_PRODUCT_DDL = """
    CREATE TABLE `{table_name}` (
    `id` int(11) NOT NULL DEFAULT 0,
    `client_product_id` varchar(50) DEFAULT NULL,
    `master_id` int(11) DEFAULT NULL,
    `master_type` varchar(20) DEFAULT NULL,
    `name` varchar(164) DEFAULT NULL,
    `generic_name` varchar(256) DEFAULT NULL,
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

# Used to reload the product table from production, and transform it.
CREATE_PRODUCT_RELOAD_DDL = """
CREATE TABLE `{table_name}` (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `client_product_id` varchar(50) DEFAULT NULL,
  `Name` varchar(100) NOT NULL,
  `Generic_Name` varchar(100) DEFAULT NULL,
  `Drug_Type` enum('RX','OTC','DISC') DEFAULT NULL,
  `Source_Type` enum('FDA','USER','FDA_DISAPPROVED') DEFAULT NULL,
  `Company_id` int(10) unsigned DEFAULT NULL,
  `Is_Admin_Approved` bit(1) DEFAULT NULL,
  `verified_source` varchar(30) DEFAULT NULL,
  `verified_source_id` varchar(20) DEFAULT NULL,
  `Created_By` mediumint(9) NOT NULL,
  `Created_Date` datetime NOT NULL,
  `Modified_By` mediumint(9) DEFAULT NULL,
  `Modified_Date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `source` (`verified_source_id`,`verified_source`),
  KEY `Company_ID` (`Company_id`),
  FULLTEXT KEY `product_name_generic_name_index` (`Name`,`Generic_Name`)
) ENGINE=InnoDB AUTO_INCREMENT=13613 DEFAULT CHARSET=latin1;
"""