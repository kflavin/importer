create procedure sp_prep_drugs_param(IN prodDb varchar(255))
BEGIN

    # This SP loads RXNORM data into the products table.  It is scheduled to run monthly through Talend.
    
    # prodDb specifies where the prod DB lives.  This is the database with the app tables.
    IF IFNULL(prodDb,'') <> '' THEN SET prodDb = CONCAT(prodDb, '.'); ELSE SET prodDb = ''; END IF;
    SELECT CONCAT('PROD DB=', prodDb);

    SET SQL_SAFE_UPDATES = 0;
    SET @CREATOR_ID = 1;
    SET @UPDATER_ID = 1;
    SET @APPROVER_ID = null;
    SET @DELETER_ID = null;
    SET @UKNOWN_CATEGORY_ID = 5;
    SET @DRUG_CATEGORY_ID = 1;
    SET @VACCINE_CATEGORY_ID = 8;
    SET @maxlen_generic_name = 884;

    SET @@SESSION.group_concat_max_len = 4096;

    DROP TABLE IF EXISTS tmp_products_toCompare;

    # Create a new table like products.  Use a prepared statement, so we can parameterize the DB name.
    SET @s = CONCAT('CREATE TABLE tmp_products_toCompare LIKE ', prodDb ,' products');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    # Make a copy of the products table for comparison.
    SET @s = CONCAT('INSERT INTO tmp_products_toCompare SELECT * FROM ', prodDb ,' products');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    # Create a temporary table like products, for new RXNORM data.
    DROP TABLE IF EXISTS tmp_products;
    CREATE TABLE tmp_products like tmp_products_toCompare;
    
    SELECT 'CREATE tmp_products FROM RXNORM DATA';
    # Bring the new data from RXNORM into tmp_products
    INSERT INTO tmp_products (`name`, `generic_name`, `rxcui_id`, `source`, `product_category_id`, `is_generic`, `approver_id`, `creator_id`, `deleter_id`, `updater_id`, `created_at`, `updated_at`)
    SELECT DISTINCT t2.BrandName as Name,
                    SUBSTRING(GROUP_CONCAT(distinct t3.STR ORDER BY t3.STR ASC SEPARATOR ', '), 1, @maxlen_generic_name) as GenericName,
                    t2.BrandRXCUI, 'RXNORM', @DRUG_CATEGORY_ID, 0, @APPROVER_ID, @CREATOR_ID, @DELETER_ID, @UPDATER_ID, NOW(), NOW() # Find all TTY=IN from RXNORM vocab and print Brand/Generic names
    FROM stage_rxnconso t3
    JOIN (
      SELECT t1.RXCUI as BrandRXCUI,t1.STR as BrandName,t2.RXCUI2 as GenericRXCUI, RELA  # Lookup all ingredients with "has_tradename" relationship w/ Brand Name
      FROM (
       SELECT `RXCUI`, `STR` FROM stage_rxnconso WHERE SAB='RXNORM' and TTY='BN' AND STR NOT LIKE '%,%'  # find brand names, generic names their RXCUI
      ) t1 JOIN stage_rxnrel t2 ON t1.RXCUI = t2.RXCUI1 WHERE RELA='has_tradename'
    ) t2 ON t3.RXCUI = t2.GenericRXCUI WHERE SAB='RXNORM' AND TTY='IN' GROUP BY BrandRXCUI
    UNION
    SELECT DISTINCT STR as Name, STR as GenericName, RXCUI, 'RXNORM', @DRUG_CATEGORY_ID, 1, @APPROVER_ID, @CREATOR_ID, @DELETER_ID, @UPDATER_ID, NOW(), NOW()
    FROM stage_rxnconso WHERE SAB='RXNORM' and TTY='IN' and STR not like '%,%';
    
    SELECT 'INSERT INTO tmp_products2';
    # Create a copy of this table to use with the synonyms
    DROP TABLE IF EXISTS tmp_products2;
    CREATE TABLE tmp_products2 LIKE tmp_products;
    INSERT INTO tmp_products2 SELECT * FROM tmp_products;
    
    # Delete all but the new records
    DELETE t1 FROM tmp_products t1 JOIN tmp_products_toCompare t2
        ON t1.rxcui_id = t2.rxcui_id;

    SELECT 'UPDATE VACCINES IN tmp_products WITH VACCINE CATEGORY';
    # Anything with "vaccine" in the name, set to category 8 (vaccine)
    UPDATE tmp_products
    SET product_category_id = 8
    WHERE name LIKE '%vaccine%' or generic_name LIKE '%vaccine%';

    SELECT 'INSERT INTO products FROM tmp_products';
    # Use a prepared statement, so we can parameterize the DB name.
    SET @s = CONCAT('INSERT INTO ', prodDb, 'products'
        '(`name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `is_generic`, `approver_id`, `creator_id`, `deleter_id`, `updater_id`, `created_at`, `updated_at`) ',
        'SELECT DISTINCT `name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `is_generic`, `approver_id`, `creator_id`, `deleter_id`, `updater_id`, NOW(), NOW() from tmp_products'
        );
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    SELECT 'CREATE tmp_product_synonyms_toCompare';
    # Make a copy of the synonyms 
    DROP TABLE IF EXISTS tmp_product_synonyms_toCompare;
    SET @s = CONCAT('CREATE TABLE tmp_product_synonyms_toCompare like ', prodDb ,'product_synonyms');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    SELECT 'INSERT INTO tmp_product_synonyms_toCompare';
    # Make a copy of the products table for comparison.
    SET @s = CONCAT('INSERT INTO tmp_product_synonyms_toCompare SELECT * FROM ', prodDb ,'product_synonyms');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    DROP TABLE IF EXISTS tmp_product_synonyms;
    CREATE TABLE tmp_product_synonyms like tmp_product_synonyms_toCompare;
    
    SELECT 'INSERT INTO tmp_product_synonyms';
    INSERT INTO tmp_product_synonyms (`rxcui_id`, `synonym`, `creator_id`, `updater_id`, `created_at`, `updated_at`)
    SELECT brand_rxcui, STR, @CREATOR_ID, @UPDATER_ID, NOW(), NOW() from stage_rxnconso t3 JOIN (
    SELECT name,t1.rxcui_id as brand_rxcui,t2.rxcui2 as generic_rxcui FROM tmp_products2 t1 JOIN stage_rxnrel t2 on t1.rxcui_id = t2.rxcui1 WHERE (RELA='precise_ingredient_of' or RELA='has_tradename') and is_generic = 0
    ) t4 on t3.rxcui = t4.generic_rxcui WHERE (TTY='SY' or TTY='TMSY' or TTY='PIN' or TTY='IN') and  SAB='RXNORM';
    
    SELECT 'DELETE FROM SYNONYMS';
    DELETE t1 FROM tmp_product_synonyms t1 JOIN tmp_product_synonyms_toCompare t2 ON t1.rxcui_id = t2.rxcui_id;

    SELECT 'INSERT NEW SYNONYMS';
    SET @s = CONCAT('INSERT INTO ', prodDb, 'product_synonyms'
        '(`rxcui_id`, `synonym`, `creator_id`, `updater_id`, `created_at`, `updated_at`) ',
        'SELECT `rxcui_id`, `synonym`, `creator_id`, `updater_id`, `created_at`, `updated_at` from tmp_product_synonyms'
        );
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

END;

