# Perform a delta on the NDC master and product master tables
# This should return a result of the format: `present`, <primary key>
# where order of the columns is important
DELTA_PRODUCTMASTER_TO_PRODUCT_Q = """
SELECT CASE WHEN r.master_id IS NULL THEN false ELSE true END as present, l.master_id, l.master_type
FROM {left_table_name} l
LEFT OUTER JOIN {right_table_name}  r
ON l.master_id = r.master_id AND l.master_type = r.master_type GROUP BY master_id, master_type
"""

RETRIEVE_PRODUCT_Q = """
    SELECT
    `id`,
    `client_product_id`,
    `master_id`,
    `master_type`,
    `name`,
    `generic_name`,
    `drug_type`,
    `source_type`,
    `company_id`,
    `is_admin_approved`,
    `verified_source`,
    `verified_source_id`,
    `created_by`,
    `created_date`,
    `modified_by`,
    `modified_date`,
    `eff_date`,
    `end_date`
    FROM  {table_name}
    WHERE (
		{where_clause}
    ) GROUP BY master_id
"""

RETRIEVE_PRODUCTMASTER_Q = """
    SELECT
    `id`,
    `master_id`,
    `master_type`,
    `proprietaryname`,
    `nonproprietaryname`,
    `eff_date`,
    `end_eff_date`
    FROM  {table_name}
    WHERE (
		{where_clause}
    ) GROUP BY master_id
"""

# Insert into the product master table
INSERT_PRODUCT_MASTER_Q = """
    INSERT INTO `{table_name}` (
    `master_id`,
    `master_type`,
    `proprietaryname`,
    `nonproprietaryname`,
    `eff_date`,
    `end_eff_date`
    )
    VALUES(%(master_id)s, "Drug", %(proprietaryname)s, %(nonproprietaryname)s, DATE(NOW()), NULL)
"""

ARCHIVE_PRODUCT_MASTER_Q = """
    INSERT into {archive_table_name} (
        id, master_id, master_type, proprietaryname, nonproprietaryname, eff_date, 
        end_eff_date, created_at, updated_at)
    select id, master_id, master_type, proprietaryname, 
    nonproprietaryname, eff_date, date(now()), now(), now()
    from {table_name} WHERE ({where_clause});
"""