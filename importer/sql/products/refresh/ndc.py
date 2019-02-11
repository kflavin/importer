REFRESH_NDC_TABLE_DDL = """
CREATE TABLE {target_table_name} LIKE {source_table_name};
"""

# Add indications data (~12 minutes)
REFRESH_NDC_TABLE_LOAD_INDICATIONS = """
INSERT INTO {target_table_name} (
    labelername,
    productndc,
    proprietaryname,
    nonproprietaryname,
    producttypename,
    marketingcategoryname,
    ndc_exclude_flag,
    ind_drug_id,
    ind_drug_name,
    ind_name,
    ind_status,
    ind_phase,
    ind_detailedstatus)
SELECT 
    n.labelername, 
    n.productndc, 
    n.proprietaryname, 
    n.nonproprietaryname, 
    n.producttypename, 
    n.marketingcategoryname, 
    n.ndc_exclude_flag, 
    i.drug_id, 
    i.drug_name, 
    i.ind_name, 
    i.status, 
    i.phase, 
    i.detailedstatus
FROM {indications_table_name} i 
JOIN {ndc_product_table_name} n 
ON i.drug_name = n.proprietaryname or i.drug_name = n.nonproprietaryname
"""
# Change ON from this:
# ON LOWER(i.drug_name) = LOWER(n.proprietaryname)
# Collation not case-sensitive, and using the index is much faster.  Also, nonproprietary name also matches
# the indications table for some drugs.

# # Add master id back in
# REFRESH_NDC_TABLE_ADD_MASTER_ID = """
# UPDATE {target_table_name} n
# INNER JOIN 
# (
#     SELECT 
#         master_id, 
#         proprietaryname, 
#         nonproprietaryname 
#     FROM {productmaster_table_name}
#     WHERE master_type="Drug"
# ) AS p
# ON UPPER(n.proprietaryname) = p.proprietaryname AND UPPER(n.nonproprietaryname) = n.nonproprietaryname
# SET n.master_id = p.master_id;
# """

# Add te codes from Orange book - 110 seconds (do we need to add nonproprietary name to the join?)
# te_type may not make sense in this table.  The same drug can have different types in the orange book.
REFRESH_NDC_TABLE_LOAD_ORANGE = """
    UPDATE	{target_table_name} n
    LEFT JOIN ( 
        SELECT 
            n2.proprietaryname,
            o.te_code,
            o.type
        FROM {orange_table_name} o 
        JOIN {target_table_name} n2 
        ON n2.proprietaryname = o.trade_name 
        WHERE o.te_code IS NOT NULL 
        GROUP BY o.trade_name, o.te_code
    ) o
    ON n.proprietaryname = o.proprietaryname
    SET n.te_type =  o.type, 
        n.te_code = o.te_code
"""
# Note, removed LOWER to make use of index.  Collation doesn't seem to be case sensitive.
# ON LOWER(n.proprietaryname) = LOWER(o.proprietaryname)

# # New
# REFRESH_NDC_TABLE_LOAD_ORANGE = """
#     UPDATE	{target_table_name} n
#     LEFT JOIN ( 
#         SELECT 
#             n2.proprietaryname,
#             n2.nonproprietaryname,
#             o.te_code
#         FROM {orange_table_name} o 
#         JOIN {target_table_name} n2 
#         ON (n2.proprietaryname = o.trade_name OR n2.nonproprietaryname = o.trade_name)
#         WHERE o.te_code IS NOT NULL 
#         GROUP BY n2.proprietaryname, n2.nonproprietaryname, o.te_code
#     ) o
#     ON n.proprietaryname = o.proprietaryname AND n.nonproprietaryname = o.nonproprietaryname
#     SET n.te_code = o.te_code
# """


REFRESH_NDC_TABLE_LOAD_MARKETING = """
    UPDATE	{target_table_name} n
    LEFT JOIN ( 
        SELECT 
            n2.proprietaryname,
            o.te_code,
            o.type
        FROM {orange_table_name} o 
        JOIN {target_table_name} n2 
        ON n2.proprietaryname = o.trade_name 
        WHERE o.te_code IS NOT NULL 
        GROUP BY o.trade_name, o.te_code
    ) o
    ON n.proprietaryname = o.proprietaryname
    SET n.te_type =  o.type, 
        n.te_code = o.te_code
"""