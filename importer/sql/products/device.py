
######################################
# RXV Med Device Master
######################################

CREATE_RXV_MED_DEVICE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT DEFAULT NULL,
    `deviceid` VARCHAR(50) DEFAULT NULL,
    `deviceidtype` VARCHAR(50) DEFAULT NULL,
    `deviceidissuingagency` VARCHAR(50) DEFAULT NULL,
    `containsdinumber` VARCHAR(50) DEFAULT NULL,
    `brandname` VARCHAR(128) DEFAULT NULL,
    `dunsnumber` BIGINT DEFAULT NULL,
    `pkgdiscontinuedate` DATE DEFAULT NULL,
    `pkgstatus` VARCHAR(30) DEFAULT NULL,
    `eff_date` DATE DEFAULT NULL,
    `end_eff_date` DATE DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
    );
"""

CREATE_RXV_MED_DEVICE_COMPLETE  = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT DEFAULT NULL,
    `primarydi` VARCHAR(50) DEFAULT NULL,
    `deviceid` VARCHAR(50) DEFAULT NULL,
    `deviceidtype` VARCHAR(50) DEFAULT NULL,
    `devicedescription` VARCHAR(2000) DEFAULT NULL,
    `companyname` VARCHAR(120) DEFAULT NULL,
    `phone` VARCHAR(20) DEFAULT NULL,
    `phoneextension` INT DEFAULT NULL,
    `email` VARCHAR(100) DEFAULT NULL,
    `brandname` VARCHAR(80) DEFAULT NULL,
    `dunsnumber` BIGINT DEFAULT NULL,
    `deviceidissuingagency` VARCHAR(50) DEFAULT NULL,
    `containsdinumber` BIGINT DEFAULT NULL,
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
    PRIMARY KEY (`id`)
    );
"""


