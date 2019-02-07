
######################################
# RXV Med Device Master
######################################

# CREATE_RXV_MED_DEVICE = """
CREATE_DEVICE_DDL = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `rx_id` INT DEFAULT NULL,
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
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

# CREATE_RXV_MED_DEVICE_COMPLETE  = """
CREATE_DEVICEMASTER_DDL  = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `primarydi` VARCHAR(50) DEFAULT NULL,
    `deviceid` VARCHAR(50) DEFAULT NULL,
    `deviceidtype` VARCHAR(50) DEFAULT NULL,
    `devicedescription` VARCHAR(2000) DEFAULT NULL,
    `companyname` VARCHAR(175) DEFAULT NULL,
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
    KEY `idx_primarydi_di_ditype` (`primarydi`, `deviceid`, `deviceidtype`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8;
"""

CREATE_DEVICEMASTER_STAGE_DML  = """
INSERT INTO `{table_name}` (
    primarydi, 
    deviceid, 
    deviceidtype, 
    devicedescription, 
    companyname, 
    phone, 
    phoneextension, 
    email, 
    brandname,
    dunsnumber,
    deviceidissuingagency,
    containsdinumber,
    pkgquantity,
    pkgdiscontinuedate,
    pkgstatus,
    rx,
    otc
) select 
    d.primarydi, 
    i.deviceid, 
    i.deviceidtype, 
    d.devicedescription, 
    d.companyname, 
    c.phone, 
    c.phoneextension, 
    c.email, 
    d.brandname,
    d.dunsnumber,
    i.deviceidissuingagency,
    i.containsdinumber,
    i.pkgquantity,
    i.pkgdiscontinuedate,
    i.pkgstatus,
    d.rx,
    d.otc
    FROM `{refresh_devices_table_name}` d
    JOIN `{refresh_identifiers_table_name}` i
    ON d.primarydi = i.primarydi
    LEFT JOIN `{refresh_contacts_table_name}` c
    ON i.primarydi = c.primarydi;
"""

# Deprecated
# DELTA_RECORDS_Q = """
#     select a.publicdevicerecordkey, a.deviceid, a.deviceidtype, CASE WHEN b.publicdevicerecordkey IS NULL THEN false ELSE true END as r 
#     from {stage_table} a 
#     left outer join {prod_table} b 
#     on a.publicdevicerecordkey = b.publicdevicerecordkey and 
#     a.deviceid = b.deviceid and 
#     a.deviceidtype = b.deviceidtype
# """

# RETRIEVE_RECORDS_Q = """
#     SELECT
#     `publicdevicerecordkey`,
#     `deviceid`,
#     `deviceidtype`,
#     `devicedescription`,
#     `companyname`,
#     `phone`,
#     `phoneextension`,
#     `email`,
#     `brandname`,
#     `dunsnumber`,
#     `deviceidissuingagency`,
#     `containsdinumber`,
#     `pkgquantity`,
#     `pkgdiscontinuedate`,
#     `pkgstatus`,
#     `pkgtype`,
#     `rx`,
#     `otc`
#     FROM  {table_name}
#     WHERE (
# 		{where_clause}
#     )
# """

# INSERT_Q = """
#     INSERT INTO `{table_name}` (
#     `publicdevicerecordkey`,
#     `deviceid`,
#     `deviceidtype`,
#     `devicedescription`,
#     `companyname`,
#     `phone`,
#     `phoneextension`,
#     `email`,
#     `brandname`,
#     `dunsnumber`,
#     `deviceidissuingagency`,
#     `containsdinumber`,
#     `pkgquantity`,
#     `pkgdiscontinuedate`,
#     `pkgstatus`,
#     `pkgtype`,
#     `rx`,
#     `otc`
#     )
#     VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
# """
