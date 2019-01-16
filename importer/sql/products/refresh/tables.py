
DROP_TABLE_DDL = """
DROP TABLE `{table_name}`;
"""

CREATE_NDC_DDL ="""
CREATE TABLE `{table_name}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `productid` VARCHAR(47) DEFAULT NULL,
  `productndc` VARCHAR(10) DEFAULT NULL,
  `producttypename` VARCHAR(27) DEFAULT NULL,
  `proprietaryname` VARCHAR(226) DEFAULT NULL,
  `proprietarynamesuffix` VARCHAR(256) DEFAULT NULL,
  `nonproprietaryname` VARCHAR(512) DEFAULT NULL,
  `dosageformname` VARCHAR(46) DEFAULT NULL,
  `routename` VARCHAR(143) DEFAULT NULL,
  `startmarketingdate` INT DEFAULT NULL,
  `endmarketingdate` INT DEFAULT NULL,
  `marketingcategoryname` VARCHAR(40) DEFAULT NULL,
  `applicationnumber` VARCHAR(15) DEFAULT NULL,
  `labelername` VARCHAR(121) DEFAULT NULL,
  `substancename` VARCHAR(3814) DEFAULT NULL,
  `active_numerator_strength` VARCHAR(742) DEFAULT NULL,
  `active_ingred_unit` VARCHAR(2055) DEFAULT NULL,
  `pharm_classes` VARCHAR(3998) DEFAULT NULL,
  `deaschedule` VARCHAR(4) DEFAULT NULL,
  `ndc_exclude_flag` VARCHAR(1) DEFAULT NULL,
  `listing_record_certified_through` INT DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_reload_ndc_product_proprietaryname` (`proprietaryname`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_ORANGE_DDL = """
CREATE TABLE `{table_name}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `ingredient` VARCHAR(233) DEFAULT NULL,
  `df_route` VARCHAR(79) DEFAULT NULL,
  `trade_name` VARCHAR(97) DEFAULT NULL,
  `applicant` VARCHAR(20) DEFAULT NULL,
  `strength` VARCHAR(206) DEFAULT NULL,
  `appl_type` VARCHAR(1) DEFAULT NULL,
  `appl_no` INT DEFAULT NULL,
  `product_no` INT DEFAULT NULL,
  `te_code` VARCHAR(15) DEFAULT NULL,
  `approval_date` VARCHAR(29) DEFAULT NULL,
  `rld` VARCHAR(3) DEFAULT NULL,
  `rs` VARCHAR(3) DEFAULT NULL,
  `type` VARCHAR(5) DEFAULT NULL,
  `applicant_full_name` VARCHAR(88) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_reload_orange_trade_name` (`trade_name`),
   KEY `idx_reload_orange_te_code` (`te_code`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_INDICATIONS_DDL = """
CREATE TABLE `{table_name}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `drug_name` VARCHAR(47) DEFAULT NULL,
  `drug_id` VARCHAR(7) DEFAULT NULL,
  `ind_name` VARCHAR(105) DEFAULT NULL,
  `ind_id` VARCHAR(8) DEFAULT NULL,
  `NCT` VARCHAR(11) DEFAULT NULL,
  `status` VARCHAR(10) DEFAULT NULL,
  `phase` VARCHAR(15) DEFAULT NULL,
  `DetailedStatus` VARCHAR(163) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_reload_indications_drug_name` (`drug_name`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_MARKETING_DDL = """
CREATE TABLE `{table_name}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `spl_acceptable_term` VARCHAR(62) DEFAULT NULL,
  `code` VARCHAR(7) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
   PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_MEDICAL_DEVICE_MASTER_DDL = """
  CREATE TABLE IF NOT EXISTS `{table_name}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `primarydi` VARCHAR(50) DEFAULT NULL,
  `deviceid` VARCHAR(50) DEFAULT NULL,
  `deviceidtype` VARCHAR(50) DEFAULT NULL,
  `devicedescription` VARCHAR(3000) DEFAULT NULL,
  `companyname` VARCHAR(120) DEFAULT NULL,
  `phone` VARCHAR(20) DEFAULT NULL,
  `phoneextension` BIGINT DEFAULT NULL,
  `email` VARCHAR(100) DEFAULT NULL,
  `brandname` VARCHAR(175) DEFAULT NULL,
  `dunsnumber` BIGINT DEFAULT NULL,
  `deviceidissuingagency` VARCHAR(50) DEFAULT NULL,
  `containsdinumber` VARCHAR(23) DEFAULT NULL,
  `pkgquantity` INT DEFAULT NULL,
  `pkgdiscontinuedate` DATE DEFAULT NULL,
  `pkgstatus` VARCHAR(50) DEFAULT NULL,
  `pkgtype` VARCHAR(20) DEFAULT NULL,
  `rx` BOOL DEFAULT NULL,
  `otc` BOOL DEFAULT NULL,
  `eff_date` DATE DEFAULT NULL,
  `end_eff_date` DATE DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_deviceid_deviceidtype` (`deviceid`, `deviceidtype`)
  ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8
"""

# CREATE_MEDICAL_DEVICE_MASTER_DDL = """
#   CREATE TABLE IF NOT EXISTS `{table_name}` (
# 	`publicdevicerecordkey` VARCHAR(50) DEFAULT NULL,
# 	`deviceid` VARCHAR(50) DEFAULT NULL,
#   `deviceidtype` VARCHAR(50) DEFAULT NULL,
#   `devicedescription` VARCHAR(3000) DEFAULT NULL,
#   `companyname` VARCHAR(120) DEFAULT NULL,
#   `phone` VARCHAR(20) DEFAULT NULL,
#   `phoneextension` BIGINT DEFAULT NULL,
#   `email` VARCHAR(100) DEFAULT NULL,
#   `brandname` VARCHAR(175) DEFAULT NULL,
#   `dunsnumber` BIGINT DEFAULT NULL,
#   `deviceidissuingagency` VARCHAR(50) DEFAULT NULL,
#   `containsdinumber` VARCHAR(23) DEFAULT NULL,
#   `pkgquantity` INT DEFAULT NULL,
#   `pkgdiscontinuedate` DATE DEFAULT NULL,
#   `pkgstatus` VARCHAR(50) DEFAULT NULL,
#   `pkgtype` VARCHAR(20) DEFAULT NULL,
#   `rx` BOOL DEFAULT NULL,
#   `otc` BOOL DEFAULT NULL,
#   `eff_date` DATE DEFAULT NULL,
#   `end_eff_date` DATE DEFAULT NULL,
#   `created_at` datetime DEFAULT NULL,
#   `updated_at` datetime DEFAULT NULL,
#   PRIMARY KEY (`publicdevicerecordkey`, `deviceid`, `deviceidtype`)
#   ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8
# """