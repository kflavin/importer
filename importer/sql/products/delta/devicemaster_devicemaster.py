DELTA_DEVICEMASTER_DEVICEMASTER_Q = """
    SELECT 
        CASE WHEN r.primarydi IS NULL THEN false ELSE true END as present,
        l.primarydi, 
        l.deviceid, 
        l.deviceidtype
    FROM {left_table_name} l
    LEFT OUTER JOIN {right_table_name} r
    on l.primarydi = r.primarydi AND 
       l.deviceid <=> r.deviceid AND 
       l.deviceidtype <=> r.deviceidtype
"""

RETRIEVE_DEVICEMASTER_Q = """
    SELECT
    `primarydi`,
    `deviceid`,
    `deviceidtype`,
    `devicedescription`,
    `companyname`,
    `phone`,
    `phoneextension`,
    `email`,
    `brandname`,
    `dunsnumber`,
    `deviceidissuingagency`,
    `containsdinumber`,
    `pkgquantity`,
    `pkgdiscontinuedate`,
    `pkgstatus`,
    `pkgtype`,
    `rx`,
    `otc`
    FROM  {table_name}
    WHERE (
		{where_clause}
    )
"""

INSERT_DEVICEMASTER_Q = """
    INSERT INTO `{table_name}` (
    `primarydi`,
    `deviceid`,
    `deviceidtype`,
    `devicedescription`,
    `companyname`,
    `phone`,
    `phoneextension`,
    `email`,
    `brandname`,
    `dunsnumber`,
    `deviceidissuingagency`,
    `containsdinumber`,
    `pkgquantity`,
    `pkgdiscontinuedate`,
    `pkgstatus`,
    `pkgtype`,
    `rx`,
    `otc`,
    `eff_date`, 
    `end_eff_date`, 
    `created_at`, 
    `updated_at`
    )
    VALUES(
      %(primarydi)s,
      %(deviceid)s,
      %(deviceidtype)s,
      %(devicedescription)s,
      %(companyname)s,
      %(phone)s,
      %(phoneextension)s,
      %(email)s,
      %(brandname)s,
      %(dunsnumber)s,
      %(deviceidissuingagency)s,
      %(containsdinumber)s,
      %(pkgquantity)s,
      %(pkgdiscontinuedate)s,
      %(pkgstatus)s,
      %(pkgtype)s,
      %(rx)s,
      %(otc)s,
      DATE(NOW()),
      NULL,
      NOW(),
      NOW()
    )
"""

ARCHIVE_DEVICEMASTER_Q = """
    INSERT into {archive_table_name} (
    `id`,
    `primarydi`,
    `deviceid`,
    `deviceidtype`,
    `devicedescription`,
    `companyname`,
    `phone`,
    `phoneextension`,
    `email`,
    `brandname`,
    `dunsnumber`,
    `deviceidissuingagency`,
    `containsdinumber`,
    `pkgquantity`,
    `pkgdiscontinuedate`,
    `pkgstatus`,
    `pkgtype`,
    `rx`,
    `otc`,
    `eff_date`, 
    `end_eff_date`, 
    `created_at`, 
    `updated_at`
    )
    SELECT 
        `id`,
        `primarydi`,
        `deviceid`,
        `deviceidtype`,
        `devicedescription`,
        `companyname`,
        `phone`,
        `phoneextension`,
        `email`,
        `brandname`,
        `dunsnumber`,
        `deviceidissuingagency`,
        `containsdinumber`,
        `pkgquantity`,
        `pkgdiscontinuedate`,
        `pkgstatus`,
        `pkgtype`,
        `rx`,
        `otc`,
        `eff_date`, 
        DATE(NOW()), 
        NOW(), 
        NOW()
    FROM {table_name} WHERE ({where_clause});
"""