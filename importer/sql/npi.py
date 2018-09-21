######################################
# Queries
######################################

# GET_FILES = """
#     SELECT * FROM `{table_name}`
#     WHERE period = '{period}' AND imported = false
#     ORDER BY downloaded_at DESC
#     LIMIT {limit}
# """

# Use a subquery so we limit the search to only recently downloaded files.
GET_FILES = """
    SELECT * FROM (
        SELECT * FROM `{table_name}`
        WHERE `period`='{period}' AND `environment`='{environment}'
        ORDER BY downloaded_at DESC
        LIMIT {limit}
    ) as t
    WHERE t.imported = false
    ORDER BY downloaded_at DESC
"""

# # For importing single, user-specified files
# GET_FILE = """
#     SELECT * FROM `{table_name}`
#     WHERE `url`='{url}' AND t.imported = false
# """

######################################
# Inserts and Updates
######################################
MARK_AS_IMPORTED = """
    UPDATE `{table_name}`
    SET imported = true, attempts = attempts + 1
    WHERE id = {id};
"""

INSERT_QUERY = """
    INSERT INTO {table_name}
    ({cols})
    VALUES ({values})
    ON DUPLICATE KEY UPDATE
    {on_dupe_values}
"""

INSERT_LARGE_QUERY = """
    LOAD DATA LOCAL INFILE '{infile}' INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
"""

# Insert a new row into the import log table
INSERT_NEW_FILE = """
    INSERT INTO {table_name}
    ({cols})
    VALUES ({values})
"""

######################################
# Create Tables
######################################

# create the import log table
CREATE_NPI_IMPORT_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `url` VARCHAR(150) NOT NULL,
    `environment` VARCHAR(50) NOT NULL,
    `imported` BOOL DEFAULT FALSE,
    `attempts` INT DEFAULT 0,
    `period` CHAR(1),
    `imported_at` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `downloaded_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `url_per_env` (`url`,`environment`)
    )
"""

# create the NPI table
CREATE_NPI_TABLE = """
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    `NPI` int(11) NOT NULL,
    `Entity_Type_Code` int(1) DEFAULT NULL,
    `Replacement_NPI` int(11) DEFAULT NULL,
    `Employer_Identification_Number_EIN` varchar(9) DEFAULT NULL,
    `Provider_Organization_Name_Legal_Business_Name` varchar(70) DEFAULT NULL,
    `Provider_Last_Name_Legal_Name` varchar(35) DEFAULT NULL,
    `Provider_First_Name` varchar(20) DEFAULT NULL,
    `Provider_Middle_Name` varchar(20) DEFAULT NULL,
    `Provider_Name_Prefix_Text` varchar(5) DEFAULT NULL,
    `Provider_Name_Suffix_Text` varchar(5) DEFAULT NULL,
    `Provider_Credential_Text` varchar(20) DEFAULT NULL,
    `Provider_Other_Organization_Name` varchar(70) DEFAULT NULL,
    `Provider_Other_Organization_Name_Type_Code` varchar(1) DEFAULT NULL,
    `Provider_Other_Last_Name` varchar(35) DEFAULT NULL,
    `Provider_Other_First_Name` varchar(20) DEFAULT NULL,
    `Provider_Other_Middle_Name` varchar(20) DEFAULT NULL,
    `Provider_Other_Name_Prefix_Text` varchar(5) DEFAULT NULL,
    `Provider_Other_Name_Suffix_Text` varchar(5) DEFAULT NULL,
    `Provider_Other_Credential_Text` varchar(20) DEFAULT NULL,
    `Provider_Other_Last_Name_Type_Code` int(1) DEFAULT NULL,
    `Provider_First_Line_Business_Mailing_Address` varchar(55) DEFAULT NULL,
    `Provider_Second_Line_Business_Mailing_Address` varchar(55) DEFAULT NULL,
    `Provider_Business_Mailing_Address_City_Name` varchar(40) DEFAULT NULL,
    `Provider_Business_Mailing_Address_State_Name` varchar(40) DEFAULT NULL,
    `Provider_Business_Mailing_Address_Postal_Code` varchar(20) DEFAULT NULL,
    `Provider_Business_Mailing_Address_Country_Code` varchar(2) DEFAULT NULL,
    `Provider_Business_Mailing_Address_Telephone_Number` varchar(20) DEFAULT NULL,
    `Provider_Business_Mailing_Address_Fax_Number` varchar(20) DEFAULT NULL,
    `Provider_First_Line_Business_Practice_Location_Address` varchar(55) DEFAULT NULL,
    `Provider_Second_Line_Business_Practice_Location_Address` varchar(55) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_City_Name` varchar(40) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_State_Name` varchar(40) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_Postal_Code` varchar(20) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_Country_Code` varchar(2) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_Telephone_Number` varchar(20) DEFAULT NULL,
    `Provider_Business_Practice_Location_Address_Fax_Number` varchar(20) DEFAULT NULL,
    `Provider_Enumeration_Date` date DEFAULT NULL,
    `Last_Update_Date` date DEFAULT NULL,
    `NPI_Deactivation_Reason_Code` VARCHAR(2),
    `NPI_Deactivation_Date` date DEFAULT NULL,
    `NPI_Reactivation_Date` date DEFAULT NULL,
    `Provider_Gender_Code` varchar(1) DEFAULT NULL,
    `Authorized_Official_Last_Name` varchar(35) DEFAULT NULL,
    `Authorized_Official_First_Name` varchar(20) DEFAULT NULL,
    `Authorized_Official_Middle_Name` varchar(20) DEFAULT NULL,
    `Authorized_Official_Title_or_Position` varchar(35) DEFAULT NULL,
    `Authorized_Official_Telephone_Number` varchar(20) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_1` varchar(10) DEFAULT NULL,
    `Provider_License_Number_1` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_1` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_1` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_2` varchar(10) DEFAULT NULL,
    `Provider_License_Number_2` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_2` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_2` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_3` varchar(10) DEFAULT NULL,
    `Provider_License_Number_3` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_3` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_3` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_4` varchar(10) DEFAULT NULL,
    `Provider_License_Number_4` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_4` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_4` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_5` varchar(10) DEFAULT NULL,
    `Provider_License_Number_5` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_5` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_5` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_6` varchar(10) DEFAULT NULL,
    `Provider_License_Number_6` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_6` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_6` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_7` varchar(10) DEFAULT NULL,
    `Provider_License_Number_7` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_7` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_7` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_8` varchar(10) DEFAULT NULL,
    `Provider_License_Number_8` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_8` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_8` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_9` varchar(10) DEFAULT NULL,
    `Provider_License_Number_9` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_9` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_9` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_10` VARCHAR (10) DEFAULT NULL,
    `Provider_License_Number_10` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_10` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_10` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_11` varchar(10) DEFAULT NULL,
    `Provider_License_Number_11` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_11` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_11` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_12` varchar(10) DEFAULT NULL,
    `Provider_License_Number_12` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_12` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_12` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_13` varchar(10) DEFAULT NULL,
    `Provider_License_Number_13` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_13` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_13` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_14` varchar(10) DEFAULT NULL,
    `Provider_License_Number_14` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_14` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_14` varchar(1) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Code_15` varchar(10) DEFAULT NULL,
    `Provider_License_Number_15` varchar(20) DEFAULT NULL,
    `Provider_License_Number_State_Code_15` varchar(2) DEFAULT NULL,
    `Healthcare_Provider_Primary_Taxonomy_Switch_15` varchar(1) DEFAULT NULL,
    `Is_Sole_Proprietor` varchar(1) DEFAULT NULL,
    `Is_Organization_Subpart` varchar(1) DEFAULT NULL,
    `Parent_Organization_LBN` varchar(70) DEFAULT NULL,
    `Parent_Organization_TIN` varchar(9) DEFAULT NULL,
    `Authorized_Official_Name_Prefix_Text` VARCHAR(5) DEFAULT NULL,
    `Authorized_Official_Name_Suffix_Text` VARCHAR(5) DEFAULT NULL,
    `Authorized_Official_Credential_Text` varchar(20) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_1` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_2` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_3` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_4` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_5` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_6` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_7` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_8` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_9` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_10` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_11` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_12` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_13` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_14` varchar(70) DEFAULT NULL,
    `Healthcare_Provider_Taxonomy_Group_15` varchar(70) DEFAULT NULL,
    `Modified_Timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`NPI`),
    KEY `Postal_Code` (`Provider_Business_Practice_Location_Address_Postal_Code`),
    KEY `LastName_PostalCode` (`Provider_Last_Name_Legal_Name`,`Provider_Business_Practice_Location_Address_Postal_Code`),
    KEY `FirstName_LastName` (`Provider_First_Name`,`Provider_Last_Name_Legal_Name`),
    KEY `Address_State` (`Provider_Business_Practice_Location_Address_State_Name`)
    )
    """