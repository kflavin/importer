DELTA_DEVICEMASTER_DEVICEMASTER_Q = """
    select a.publicdevicerecordkey, a.deviceid, a.deviceidtype, CASE WHEN b.publicdevicerecordkey IS NULL THEN false ELSE true END as r 
    from {left_table_name} l
    left outer join {right_table_name} r
    on a.publicdevicerecordkey = b.publicdevicerecordkey and 
    a.deviceid = b.deviceid and 
    a.deviceidtype = b.deviceidtype
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
    `otc`
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
    `otc`
    )
    SELECT `id`,
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
            `otc`)
    FROM {table_name} WHERE ({where_clause});
"""