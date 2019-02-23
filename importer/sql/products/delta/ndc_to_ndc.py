
DELTA_NDC_TO_NDC_Q = """
SELECT 
    CASE WHEN r.productndc IS NULL and r.ind_name IS NULL and r.te_code IS NULL and r.ind_detailedstatus IS NULL THEN false ELSE true END AS present, 
    l.productndc, 
    l.ind_name, 
    l.te_code, 
    l.ind_detailedstatus
FROM {left_table_name} l
LEFT JOIN {right_table_name} r
ON l.productndc = r.productndc 
    AND l.ind_name <=> r.ind_name 
    AND l.te_code <=> r.te_code 
    AND l.ind_detailedstatus <=> r.ind_detailedstatus
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
    )
"""

# master_id won't be present until added back from prodmaster complete (4.1.11)
# add back in definition and interpretation
INSERT_NDC_Q = """
    INSERT INTO `{table_name}` (
    `labelername`,
    `productndc`,
    `proprietaryname`,
    `nonproprietaryname`,
    `producttypename`,
    `marketingcategoryname`,
    `te_code`,
    `te_type`,
    `ndc_exclude_flag`,
    `ind_drug_id`,
    `ind_drug_name`,
    `ind_name`,
    `ind_status`,
    `ind_phase`,
    `ind_detailedstatus`,
    `eff_date`,
    `end_eff_date`,
    `created_at`,
    `updated_at`
    )
    VALUES(
      %(labelername)s,
      %(productndc)s,
      %(proprietaryname)s,
      %(nonproprietaryname)s,
      %(producttypename)s,
      %(marketingcategoryname)s,
      %(te_code)s,
      %(te_type)s,
      %(ndc_exclude_flag)s,
      %(ind_drug_id)s,
      %(ind_drug_name)s,
      %(ind_name)s,
      %(ind_status)s,
      %(ind_phase)s,
      %(ind_detailedstatus)s,
      DATE(NOW()),
      NULL,
      NOW(),
      NOW()
    )
"""

ARCHIVE_NDC_Q = """
    INSERT into {archive_table_name} (
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
      `end_eff_date`,
      `created_at`,
      `updated_at`
    )
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
          DATE(NOW()),
          NULL,
          NOW(),
          NOW()
    FROM {table_name} 
    WHERE ({where_clause});
"""