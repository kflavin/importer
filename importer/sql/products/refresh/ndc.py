REFRESH_NDC_TABLE = []

REFRESH_NDC_TABLE_DDL = """
CREATE TABLE {target_table_name} LIKE {source_table_name};
"""

# Add indications data (~12 minutes)
REFRESH_NDC_TABLE_LOAD_INDICATIONS = """
INSERT INTO {target_table_name} (labelername, productndc, proprietaryname, nonproprietaryname, producttypename, marketingcategoryname, ndc_exclude_flag, ind_drug_id, ind_drug_name, ind_name, ind_status, ind_phase, ind_detailedstatus)
SELECT n.labelername, n.productndc, n.proprietaryname, n.nonproprietaryname, n.producttypename, n.marketingcategoryname, n.ndc_exclude_flag, i.drug_id, i.drug_name, i.ind_name, i.status, i.phase, i.detailedstatus
FROM {indications_table_name} i join {ndc_product_table_name} n on lower(i.drug_name) = lower(n.proprietaryname);
"""

# Add master id back in (very long, ~9 hours)
REFRESH_NDC_TABLE_ADD_MASTER_ID = """
UPDATE {target_table_name} n
INNER JOIN 
(select master_id, proprietaryname, nonproprietaryname from {productmaster_table_name} where master_type="Drug") as p
ON UPPER(n.proprietaryname) = p.proprietaryname AND UPPER(n.nonproprietaryname) = n.nonproprietaryname
SET n.master_id = p.master_id;
"""

# Add te codes from Orange book - 110 seconds (do we need to add nonproprietary name to the join?)
REFRESH_NDC_TABLE_LOAD_ORANGE = """
INSERT INTO {target_table_name2} (master_id, labelername, productndc, proprietaryname, nonproprietaryname, producttypename, marketingcategoryname, definition, te_code, te_type, interpretation, ndc_exclude_flag, ind_drug_id, ind_drug_name, ind_name, ind_status, ind_phase, ind_detailedstatus, eff_date, end_eff_date)
SELECT n.master_id, n.labelername, n.productndc, n.proprietaryname, n.nonproprietaryname, n.producttypename, n.marketingcategoryname, n.definition, o.te_code, n.te_type, n.interpretation, n.ndc_exclude_flag, n.ind_drug_id, n.ind_drug_name, n.ind_name, n.ind_status, n.ind_phase, n.ind_detailedstatus, n.eff_date, n.end_eff_date
FROM {target_table_name} n LEFT JOIN ( SELECT n2.proprietaryname,o.te_code FROM {orange_table_name} o JOIN {target_table_name} n2 on n2.proprietaryname = o.trade_name WHERE o.te_code IS NOT NULL GROUP BY o.trade_name, o.te_code ) o
ON LOWER(n.proprietaryname) = LOWER(o.proprietaryname)
"""

# Need to add this.  It merges extraneous records with help from the te_code.
"""
create table test_ndc_reload3 like test_ndc_reload2;
insert into test_ndc_reload3 select * from test_ndc_reload2 group by productndc,ind_name,te_code,ind_detailedstatus;
"""
