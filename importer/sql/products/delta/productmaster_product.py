# Only select present items.  Don't insert items from product master complete into product master
# that aren't already there.
DELTA_PRODUCTMASTER_TO_PRODUCT_Q = """
SELECT * FROM (
SELECT CASE WHEN r.master_id IS NULL THEN false ELSE true END as present, l.master_id, l.master_type
FROM {left_table_name} l
LEFT OUTER JOIN {right_table_name}  r
ON l.master_id = r.master_id AND l.master_type = r.master_type GROUP BY master_id, master_type
) t
WHERE present = 1;
"""

# this would insert all new records as well
# DELTA_PRODUCTMASTER_TO_PRODUCT_Q = """
# SELECT CASE WHEN r.master_id IS NULL THEN false ELSE true END as present, l.master_id, l.master_type
# FROM {left_table_name} l
# LEFT OUTER JOIN {right_table_name}  r
# ON l.master_id = r.master_id AND l.master_type = r.master_type GROUP BY master_id, master_type
# """

# # Left table
# RETRIEVE_PRODUCTMASTER_Q = """
#     SELECT
#     `master_id`,
#     `master_type`,
#     `proprietaryname`,
#     `nonproprietaryname`,
#     `eff_date`,
#     `end_eff_date`
#     FROM  {table_name}
#     WHERE (
# 		{where_clause}
#     ) GROUP BY master_id
# """

# Right table
RETRIEVE_PRODUCT_Q = """
    SELECT
    `id`,
    `client_product_id`,
    `master_id`,
    `master_type`,
    `proprietaryname`,
    `nonproprietaryname`,
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
    ) GROUP BY master_id, master_type
"""

# Insert into the product table
INSERT_PRODUCT_Q = """
    INSERT INTO `{table_name}` (
        `id`,
        `client_product_id`,
        `master_id`,
        `master_type`,
        `proprietaryname`,
        `nonproprietaryname`,
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
    )
    VALUES(
        %(id)s,
        %(client_product_id)s,
        %(master_id)s,
        %(master_type)s,
        %(proprietaryname)s,
        %(nonproprietaryname)s,
        %(drug_type)s,
        %(source_type)s,
        %(company_id)s,
        %(is_admin_approved)s,
        %(verified_source)s,
        %(verified_source_id)s,
        %(created_by)s,
        %(created_date)s,
        %(modified_by)s,
        %(modified_date)s,
        DATE(NOW()),
        NULL)
"""

# Archive product table records
ARCHIVE_PRODUCT_Q = """
    INSERT into {archive_table_name} (
        `rx_id`,
        `client_product_id`,
        `master_id`,
        `master_type`,
        `proprietaryname`,
        `nonproprietaryname`,
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
        )
    SELECT
        `id` as rx_id,
        `client_product_id`,
        `master_id`,
        `master_type`,
        `proprietaryname`,
        `nonproprietaryname`,
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
        date(now())
    FROM {table_name} WHERE ({where_clause});
"""