
# Use "reload" to mean reloading from rxv database, as opposed to "refresh" from gov't source data
ALTER_PRODUCT_RELOAD_TABLE = """
ALTER TABLE {table_name}
ADD COLUMN `master_id` int(11) DEFAULT NULL AFTER `client_product_id`,
ADD COLUMN `master_type` varchar(20) DEFAULT NULL AFTER `master_id`,
ADD COLUMN `eff_date` DATE DEFAULT NULL,
ADD COLUMN `end_date` DATE DEFAULT NULL;
"""

# ALTER_PRODUCT_RELOAD_TABLE = """
# ALTER TABLE {table_name}
# ADD COLUMN `master_id` int(11) DEFAULT NULL AFTER `client_product_id`,
# ADD COLUMN `master_type` varchar(20) DEFAULT NULL AFTER `master_id`,
# CHANGE `name` `proprietaryname` VARCHAR(512),
# CHANGE `generic_name` `nonproprietaryname` VARCHAR(512),
# ADD COLUMN `eff_date` DATE DEFAULT NULL,
# ADD COLUMN `end_date` DATE DEFAULT NULL;
# """