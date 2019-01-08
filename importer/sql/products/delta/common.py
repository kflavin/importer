RETRIEVE_PRODUCTMASTER_Q = """
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