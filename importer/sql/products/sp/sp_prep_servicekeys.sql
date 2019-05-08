CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`sp_prep_servicekeys`()
BEGIN

	DROP TABLE IF EXISTS tmp_product_keys;
    DROP TABLE IF EXISTS tmp_product_keys_toCompare;
    
    CREATE TABLE tmp_product_keys (
			Product_ID varchar(50),
			Product_Name varchar(226) DEFAULT NULL,  
			ProprietaryName varchar(400) DEFAULT NULL,
			NonProprietaryName varchar(512) DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARACTER SET='utf8mb4';
    
    CREATE TABLE tmp_product_keys_toCompare (
			Product_ID varchar(50),				    
			Product_Name varchar(226) DEFAULT NULL,  
			ProprietaryName varchar(400) DEFAULT NULL,
			NonProprietaryName varchar(512) DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARACTER SET='utf8mb4';
    
    INSERT INTO tmp_product_keys (`Product_ID`, `Product_Name`, `ProprietaryName`, `NonProprietaryName`) SELECT cpt_code, service, service, service FROM stage_cms_service;
    INSERT INTO tmp_product_keys_toCompare (`Product_ID`, `Product_Name`, `ProprietaryName`, `NonProprietaryName`)  
		SELECT `Product_ID`, `Product_Name`, `ProprietaryName`, `NonProprietaryName` FROM {prod_db}.product_keys WHERE Product_Type='SERVICES';
    
    DELETE k FROM tmp_product_keys k
    JOIN tmp_product_keys_toCompare c
	ON k.Product_ID = c.Product_ID
		AND k.Product_Name = c.Product_Name;

    INSERT INTO {prod_db}.product_keys (`Product_ID`, `Product_Type`, `Product_Name`, `ProprietaryName`, `NonProprietaryName`)
		SELECT `Product_ID`, 'SERVICES', `Product_Name`, `ProprietaryName`, `NonProprietaryName` FROM tmp_product_keys;
        
	DROP TABLE IF EXISTS tmp_product_keys;
    DROP TABLE IF EXISTS tmp_product_keys_toCompare;

END