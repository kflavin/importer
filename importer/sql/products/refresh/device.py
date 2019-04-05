 
POPULATE_DEVICE_REFRESH_TABLE = """
INSERT INTO `{table_name}`
(primarydi, deviceid, deviceidtype, devicedescription, companyname, phone, phoneextension, email, brandname,
dunsnumber, deviceidissuingagency, containsdinumber, pkgquantity, pkgdiscontinuedate, pkgstatus, pkgtype, rx, otc, eff_date, end_eff_date)
select d.primarydi, i.deviceid, i.deviceidtype, d.devicedescription, d.companyname, c.phone, c.phoneextension, c.email, d.brandname,
d.dunsnumber, i.deviceidissuingagency, i.containsdinumber, i.pkgquantity, i.pkgdiscontinuedate, i.pkgstatus, i.pkgtype, d.rx, d.otc,
DATE(NOW()), DATE(NOW())
FROM `{device_table_name}` d
LEFT JOIN `{identifier_table_name}` i ON d.primarydi = i.primarydi
LEFT JOIN `{contact_table_name}` c ON d.primarydi = c.primarydi;
"""