
DELTA_Q = """
select CASE WHEN r.master_id IS NULL THEN false ELSE true END as present, l.master_id
from {left_table_name} l
left outer join {right_table_name} r
on l.master_id = r.master_id
"""

# Perform a delta on the NDC master and product master tables
# This should return a result of the format: `present`, <primary key>
# where order of the columns is important
DELTA_NDC_TO_PRODUCT_MASTER_Q = """
select present,master_id from (
select CASE WHEN r.master_id IS NULL THEN false ELSE true END as present, l.master_id
from {left_table_name} l
left outer join {right_table_name} r
on l.master_id = r.master_id) as d group by master_id
"""

# Retrieve from the NDC table to compare with product master
RETRIEVE_NDC_Q = """
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
    `type`,
    `interpretation`,
    `ndc_exclude_flag`,
    `drug_id`,
    `ind_drug_name`,
    `ind_name`,
    `status`,
    `phase`,
    `ind_detailedstatus`,
    `eff_date`,
    `end_eff_date`
    FROM  {table_name}
    WHERE (
		{where_clause}
    ) GROUP BY master_id
"""

# Retrieve from the product master table to compare with NDC table
RETRIEVE_PRODUCT_MASTER_Q = """
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