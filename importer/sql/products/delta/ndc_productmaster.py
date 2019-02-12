# Perform a delta on the NDC master and product master tables
# This should return a result of the format: `present`, <primary key>
# where order of the columns is important. ("present" column must be first)
DELTA_NDC_TO_PRODUCTMASTER_Q = """
SELECT 
    present,
    proprietaryname,
    nonproprietaryname 
FROM (
    SELECT CASE WHEN r.proprietaryname IS NULL AND r.nonproprietaryname IS NULL THEN false ELSE true END AS present,
        l.proprietaryname,
        l.nonproprietaryname
    FROM {left_table_name} l
    LEFT OUTER JOIN {right_table_name} r
    ON UPPER(l.proprietaryname) = r.proprietaryname 
        AND UPPER(l.nonproprietaryname) = r.nonproprietaryname) AS d
    GROUP BY proprietaryname, nonproprietaryname
"""

# # No longer using
# """-- new delta query for ndc to productmaster
# select 0,n.* from ndc2 n where master_id is null group by proprietaryname,nonproprietaryname
# UNION
# select 1, t2.* from product_master2 t1 join
# (select * from ndc2 where master_id is not null group by master_id) t2
# on t1.master_id = t2.master_id
# group by t2.master_id;"""

# Retrieve from the NDC table to compare with product master
# After loading the new NDC table, new rows won't have master_id's yet, so group by pro-name, non-pro name 
RETRIEVE_NDC_GROUPS_Q = """
    SELECT
    `id`,
    `master_id`,
    `labelername`,
    `productndc`,
    `proprietaryname`,
    `nonproprietaryname`,
    `producttypename`,
    `marketingcategoryname`,
    `definition`,
    `te_code`,
    `te_type`,
    `interpretation`,
    `ndc_exclude_flag`,
    `ind_drug_id`,
    `ind_drug_name`,
    `ind_name`,
    `ind_status`,
    `ind_phase`,
    `ind_detailedstatus`,
    `eff_date`,
    `end_eff_date`
    FROM  {table_name}
    WHERE (
		{where_clause}
    ) GROUP BY proprietaryname, nonproprietaryname
"""

# Retrieve from the product master table to compare with NDC table
RETRIEVE_PRODUCT_MASTER_Q = """
    SELECT
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
INSERT_PRODUCTMASTER_Q = """
    INSERT INTO `{table_name}` (
    `master_type`,
    `proprietaryname`,
    `nonproprietaryname`,
    `eff_date`,
    `end_eff_date`,
    `created_at`,
    `updated_at`
    )
    VALUES(
        "Drug", 
        UPPER(%(proprietaryname)s), 
        UPPER(%(nonproprietaryname)s), 
        DATE(NOW()), 
        NULL, 
        NOW(), 
        NOW()
    )
"""

ARCHIVE_PRODUCTMASTER_Q = """
    INSERT into {archive_table_name} (
        master_id, 
        master_type, 
        proprietaryname, 
        nonproprietaryname, 
        eff_date, 
        end_eff_date, 
        created_at, 
        updated_at
    )
    SELECT 
        id, 
        master_id, 
        master_type, 
        proprietaryname, 
        nonproprietaryname, 
        eff_date, 
        DATE(NOW()), 
        NOW(), 
        NOW()
    FROM {table_name} WHERE ({where_clause});
"""

# Update master ID's in NDC table
MASTER_IDS_TO_NDC_TABLE = """
    UPDATE {ndc_table_name} n 
    JOIN {product_table_name} p
    ON n.proprietaryname = p.proprietaryname 
        AND n.nonproprietaryname = p.nonproprietaryname
    SET 
        n.master_id = p.master_id,
        n.updated_at = DATE(NOW())
    WHERE n.master_id IS NULL
"""
