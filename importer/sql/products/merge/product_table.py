# Create the new product table.  We'll probably end up creating the new table with a migration, so the create
# and alter table statements can be removed at that time.

# A list of SQL statements to execute
PRODUCT_TABLE_MERGE = []

PRODUCT_TABLE_MERGE.append(
"""
CREATE TABLE {new_table_name} LIKE {old_table_name}; 
"""
)

PRODUCT_TABLE_MERGE.append(
"""
INSERT INTO {new_table_name} 
SELECT * FROM {old_table_name} 
GROUP BY master_id, master_type
"""
)

PRODUCT_TABLE_MERGE.append(
"""
ALTER TABLE {new_table_name} CHANGE `name` `proprietaryname` VARCHAR(128) DEFAULT NULL
"""
)

PRODUCT_TABLE_MERGE.append(
"""
ALTER TABLE {new_table_name} change `generic_name` `nonproprietaryname` VARCHAR(512) DEFAULT NULL
"""
)

PRODUCT_TABLE_MERGE.append(
"""
UPDATE {new_table_name} t1 JOIN {product_master} t2 
ON t1.master_id=t2.master_id AND t1.master_type = t2.master_type
SET t1.proprietaryname = t2.proprietaryname, t1.nonproprietaryname = t2.nonproprietaryname;
"""
)
