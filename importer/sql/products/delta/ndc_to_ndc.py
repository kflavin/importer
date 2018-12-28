
DELTA_NDC_TO_NDC_Q = """
SELECT CASE WHEN r.productndc IS NULL and r.ind_name IS NULL and r.te_code IS NULL and r.ind_detailedstatus IS NULL THEN false ELSE true END AS present, 
l.productndc, l.ind_name, l.te_code, l.ind_detailedstatus
FROM {left_table_name} l
LEFT OUTER JOIN {right_table_name} r
ON l.productndc = r.productndc AND
l.ind_name = r.ind_name AND
l.te_code = r.te_code AND
l.ind_detailedstatus <=> r.ind_detailedstatus
"""

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
    )
"""