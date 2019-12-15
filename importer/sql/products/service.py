
CREATE_SERVICEMASTER_DDL = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `cpt_code` VARCHAR(5) DEFAULT NULL,
    `service` VARCHAR(102) DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
"""
