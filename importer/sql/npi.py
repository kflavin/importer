######################################
# Queries
######################################

# Limit files to those that have been downloaded after the last successful import, so we
# don't overwrite new data with old.
GET_FILES = """
    SELECT * FROM `{table_name}` WHERE id > (
        SELECT IF(ISNULL(MAX(id)), 0, MAX(id)) AS maxid
        FROM `{table_name}`
        WHERE period='{period}'
        AND environment='{environment}'
        AND imported = true
    )
    AND imported = false
    AND environment='{environment}'
    AND period='{period}'
    ORDER BY created_at DESC, id DESC
    LIMIT {limit};
"""

# For the monthly files, just change the order so we're loading the most recent monthly file.  We
# shouldn't try to load more than one monthly file at a time.
GET_MONTHLY_FILES = """
    SELECT * FROM `{table_name}` WHERE id > (
        SELECT IF(ISNULL(MAX(id)), 0, MAX(id)) AS maxid
        FROM `{table_name}`
        WHERE period='{period}'
        AND environment='{environment}'
        AND imported = true
    )
    AND imported = false
    AND environment='{environment}'
    AND period='{period}'
    ORDER BY created_at DESC, id DESC
    LIMIT 1;
"""

######################################
# Inserts and Updates
######################################

# Mark a zip as imported in the log table
MARK_AS_IMPORTED = """
    UPDATE `{table_name}`
    SET imported = true, attempts = attempts + 1, updated_at=now()
    WHERE id = {id};
"""

# Insert rows into the NPI table
INSERT_QUERY = """
    INSERT INTO {table_name}
    ({cols}, created_at, updated_at)
    VALUES ({values}, now(), now())
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}, updated_at=now()
"""

# Used for deactivated NPI's.  We want to keep old data, so just mark them as deactivated, don't 
# zero out all data.  Update empty date values so they are inserted as NULL instead of '0000-00-00'
UPDATE_QUERY = """
    UPDATE {table_name} SET
    `npi_deactivation_date`={npi_deactivation_date},
    `npi_reactivation_date`={npi_reactivation_date},
    `last_update_date`={last_update_date},
    `provider_enumeration_date`={provider_enumeration_date}
    WHERE `npi`={npi}
"""

# Optional query which can be used for large files
INSERT_LARGE_QUERY = """
    LOAD DATA LOCAL INFILE '{infile}' INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
"""

# Insert a new row into the import log table
INSERT_NEW_FILE = """
    INSERT INTO {table_name}
    ({cols}, updated_at, created_at)
    VALUES ({values}, now(), now())
"""

######################################
# Create Tables
######################################

# Create NPI log table.  Used only for dev, refer to api-ruby migration for accurate table.
CREATE_NPI_IMPORT_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `url` varchar(255) NOT NULL,
    `environment` varchar(50) NOT NULL,
    `imported` tinyint(1) DEFAULT 0,
    `attempts` int(11) DEFAULT 0,
    `period` char(1) DEFAULT NULL,
    `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `url_per_env` (`url`,`environment`)
    )
"""

# Create NPI table.  Used only for dev, refer to api-ruby migration for accurate table.
CREATE_NPI_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `npi` int(11) NOT NULL,
    `entity_type_code` int(1) DEFAULT NULL,
    `replacement_npi` int(11) DEFAULT NULL,
    `employer_identification_number_ein` varchar(9) DEFAULT NULL,
    `provider_organization_name_legal_business_name` varchar(70) DEFAULT NULL,
    `provider_last_name_legal_name` varchar(35) DEFAULT NULL,
    `provider_first_name` varchar(20) DEFAULT NULL,
    `provider_middle_name` varchar(20) DEFAULT NULL,
    `provider_name_prefix_text` varchar(5) DEFAULT NULL,
    `provider_name_suffix_text` varchar(5) DEFAULT NULL,
    `provider_credential_text` varchar(20) DEFAULT NULL,
    `provider_other_organization_name` varchar(70) DEFAULT NULL,
    `provider_other_organization_name_type_code` varchar(1) DEFAULT NULL,
    `provider_other_last_name` varchar(35) DEFAULT NULL,
    `provider_other_first_name` varchar(20) DEFAULT NULL,
    `provider_other_middle_name` varchar(20) DEFAULT NULL,
    `provider_other_name_prefix_text` varchar(5) DEFAULT NULL,
    `provider_other_name_suffix_text` varchar(5) DEFAULT NULL,
    `provider_other_credential_text` varchar(20) DEFAULT NULL,
    `provider_other_last_name_type_code` int(1) DEFAULT NULL,
    `provider_first_line_business_mailing_address` varchar(55) DEFAULT NULL,
    `provider_second_line_business_mailing_address` varchar(55) DEFAULT NULL,
    `provider_business_mailing_address_city_name` varchar(40) DEFAULT NULL,
    `provider_business_mailing_address_state_name` varchar(40) DEFAULT NULL,
    `provider_business_mailing_address_postal_code` varchar(20) DEFAULT NULL,
    `provider_business_mailing_address_country_code` varchar(2) DEFAULT NULL,
    `provider_business_mailing_address_telephone_number` varchar(20) DEFAULT NULL,
    `provider_business_mailing_address_fax_number` varchar(20) DEFAULT NULL,
    `provider_first_line_business_practice_location_address` varchar(55) DEFAULT NULL,
    `provider_second_line_business_practice_location_address` varchar(55) DEFAULT NULL,
    `provider_business_practice_location_address_city_name` varchar(40) DEFAULT NULL,
    `provider_business_practice_location_address_state_name` varchar(40) DEFAULT NULL,
    `provider_business_practice_location_address_postal_code` varchar(20) DEFAULT NULL,
    `provider_business_practice_location_address_country_code` varchar(2) DEFAULT NULL,
    `provider_business_practice_location_address_telephone_number` varchar(20) DEFAULT NULL,
    `provider_business_practice_location_address_fax_number` varchar(20) DEFAULT NULL,
    `provider_enumeration_date` date DEFAULT NULL,
    `last_update_date` date DEFAULT NULL,
    `npi_deactivation_reason_code` varchar(2) DEFAULT NULL,
    `npi_deactivation_date` date DEFAULT NULL,
    `npi_reactivation_date` date DEFAULT NULL,
    `provider_gender_code` varchar(1) DEFAULT NULL,
    `authorized_official_last_name` varchar(35) DEFAULT NULL,
    `authorized_official_first_name` varchar(20) DEFAULT NULL,
    `authorized_official_middle_name` varchar(20) DEFAULT NULL,
    `authorized_official_title_or_position` varchar(35) DEFAULT NULL,
    `authorized_official_telephone_number` varchar(20) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_1` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_1` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_2` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_2` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_3` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_3` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_4` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_4` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_5` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_5` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_6` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_6` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_7` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_7` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_8` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_8` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_9` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_9` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_10` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_10` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_11` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_11` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_12` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_12` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_13` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_13` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_14` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_14` varchar(1) DEFAULT NULL,
    `healthcare_provider_taxonomy_code_15` varchar(10) DEFAULT NULL,
    `healthcare_provider_primary_taxonomy_switch_15` varchar(1) DEFAULT NULL,
    `is_sole_proprietor` varchar(1) DEFAULT NULL,
    `is_organization_subpart` varchar(1) DEFAULT NULL,
    `parent_organization_lbn` varchar(70) DEFAULT NULL,
    `parent_organization_tin` varchar(9) DEFAULT NULL,
    `authorized_official_name_prefix_text` varchar(5) DEFAULT NULL,
    `authorized_official_name_suffix_text` varchar(5) DEFAULT NULL,
    `authorized_official_credential_text` varchar(20) DEFAULT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `npi` (`npi`),
    KEY `postal_code` (`provider_business_practice_location_address_postal_code`),
    KEY `lastname_postalcode` (`provider_last_name_legal_name`,`provider_business_practice_location_address_postal_code`),
    KEY `firstname_lastname` (`provider_first_name`,`provider_last_name_legal_name`),
    KEY `address_state` (`provider_business_practice_location_address_state_name`)
    )
"""