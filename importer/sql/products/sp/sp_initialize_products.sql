create procedure sp_initialize_products()
sp_create_initial_tables:BEGIN

    # This SP performs the initial migration from the old "product" table to the new "products" table.
    # It should only be run once.  It will do the following:
    #   - Load old product table with the same id's (where records do not have a match in RXNORM)
    #   - Load new data from RXNORM
    #   - Update tables dependent on old `product` table id with new id in the `products` table (if the row was merged)
        
    SET SQL_SAFE_UPDATES = 0;
    SET FOREIGN_KEY_CHECKS = 0; # There are FK checks that fail.

    SET @CREATOR_ID = 1;
    SET @UPDATER_ID = 1;
    SET @APPROVER_ID = null;
    SET @DELETER_ID = null;
    SET @UKNOWN_CATEGORY_ID = 5;
    SET @DRUG_CATEGORY_ID = 1;
    SET @VACCINE_CATEGORY_ID = 8;

    SET @products_rows = (SELECT count(1) FROM products);
    SET @products_to_product_rows := (SELECT count(1) FROM products_to_product);
    SET @product_synonym_rows := (SELECT count(1) FROM product_synonyms);
    SET @user_products_rows = (SELECT count(1) FROM user_products);

    # Sanity checks.  Don't rerun if these tables have data.
    IF @products_rows > 0 OR
        @products_to_product_rows > 0 OR
        @product_synonym_rows > 0 OR
        @user_products_rows > 0 THEN
        SELECT 'THIS SP IS FOR INITIALIZATION ONLY.  EXPECTED "PRODUCTS", "PRODUCTS_TO_PRODUCT", "USER_PRODUCT", and "PRODUCT_SYNONYMS" TABLES TO BE EMPTY.';
        LEAVE sp_create_initial_tables;
    END IF;

    # For generic names that have many ingredients
    SET @@SESSION.group_concat_max_len = 4096;

    # Get the size of the generic_name column to make sure we don't overrun it.
    SET @maxlen_generic_name = (SELECT character_maximum_length
    FROM   information_schema.columns
    WHERE  table_schema = database() AND table_name = 'products' AND column_name = 'generic_name');

    DROP TABLE IF EXISTS tmp_products;
    CREATE TABLE tmp_products like products;

    # Initial load of RXNORM data.  Generic names longer than @maxlen_generic_name are truncated to match the column
    # width.  Only 2 rows overrun the column (Prevnar).
    SELECT 'LOAD TMP_PRODUCTS';
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
    SELECT DISTINCT STR as Name, STR as GenericName, RXCUI, 'RXNORM', @DRUG_CATEGORY_ID, 1, @APPROVER_ID, @CREATOR_ID, @DELETER_ID, @UPDATER_ID, NOW(), NOW() FROM stage_rxnconso WHERE SAB='RXNORM' and TTY='IN' and STR not like '%,%';  # exclude Generics that have commas in the name

    # Put anything with "vaccine" in the name in the VACCINE category
    UPDATE tmp_products
    SET product_category_id = @VACCINE_CATEGORY_ID
    WHERE name LIKE '%vaccine%' or generic_name LIKE '%vaccine%';

    DROP TABLE IF EXISTS tmp_product_cleaned;
    CREATE TABLE `tmp_product_cleaned` (
      `id` int NOT NULL AUTO_INCREMENT,
      `Name` varchar(100) NOT NULL,
      `Generic_Name` varchar(100) DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `idx_tmp_product_cleaned_name_generic_name` (`name`,`generic_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    
    DROP TABLE IF EXISTS tmp_products_cleaned;
    CREATE TABLE `tmp_products_cleaned` (
      `id` int NOT NULL AUTO_INCREMENT,
      `name` varchar(140) DEFAULT NULL,
      `generic_name` varchar(884) DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `idx_tmp_products_cleaned_name_generic_name` (`name`,`generic_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    # Normalize names in the product table by removing non-alphanumerics.  This gets several hundred more matches
    # with the RXNORM data.
    SELECT 'NORMALIZE NAMES';
    # Normalize names in product and tmp_products for comparison
    INSERT INTO tmp_product_cleaned (`id`, `name`, `generic_name`)
    SELECT `id`,
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', '')),
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generic_name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', ''))
    from product;
    
    INSERT INTO tmp_products_cleaned (`id`, `name`, `generic_name`)
    SELECT `id`,
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', '')),
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generic_name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', ''))
    FROM tmp_products;
    
    DROP TABLE IF EXISTS tmp_products_unknown;
    CREATE TABLE tmp_products_unknown like products;

    SELECT 'INSERT TMP_PRODUCTS_UNKNOWNS';
    # Build "unknowns" table.  These are rows that don't match anything in RXNORM
    INSERT INTO tmp_products_unknown (`id`, `name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `creator_id`,`created_at`, `approver_id`, `deleter_id`, `updater_id`, `updated_at`)
    SELECT id, Name, Generic_Name, '', null, 'USER', @UKNOWN_CATEGORY_ID, @CREATOR_ID, `Created_Date`, @APPROVER_ID, @DELETER_ID, @UPDATER_ID, NOW() FROM product
    WHERE id not in (SELECT t1.id as old_id FROM tmp_product_cleaned t1 JOIN tmp_products_cleaned t2 ON t1.name = t2.name);  # WHERE finds old_id with name matches

    SELECT 'INSERT UNKNOWNS';
    # Insert the "unknowns" into the new product table.  Insert these before the RXNORM data, so they keep the same ID's.
    INSERT INTO products SELECT * FROM tmp_products_unknown;

    SELECT 'INSERT RXNORM';
    # Insert the RXNORM data into the new product table.
    INSERT INTO products (`name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `creator_id`,`created_at`, `approver_id`, `deleter_id`, `updater_id`, `updated_at`)
    SELECT `name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `creator_id`,`created_at`, `approver_id`, `deleter_id`, `updater_id`, `updated_at` FROM tmp_products;

    # Build lookup table.
    DROP TABLE IF EXISTS tmp_products_to_product;
    CREATE TABLE tmp_products_to_product like products_to_product;

    SELECT 'POPULATE LOOKUP TABLE';
    # Reload this table to get the new ID's for the RXNORM rows
    TRUNCATE tmp_products_cleaned;
    insert into tmp_products_cleaned (`id`, `name`, `generic_name`)
    select `id`,
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', '')),
    trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generic_name, '/', ''), ' ', ''), '%', ''), '-', ''), '''', ''), '"', ''), '.', ''), '(', ''), ')', ''), '+', ''), ',', ''), '#', ''), '!', ''), ';', ''), '@', ''), '&', ''), '*', ''), ':', ''), '=', ''), '–', ''))
    from products where SOURCE='RXNORM';

    # Populate lookup table with new id to old id
    INSERT INTO tmp_products_to_product (`new_id`, `old_id`)
    SELECT t2.id as new_id, t1.id as old_id FROM tmp_product_cleaned t1 JOIN tmp_products_cleaned t2 ON t1.name = t2.name;

    INSERT INTO products_to_product SELECT * FROM tmp_products_to_product;

    ############################################################
    # Populate new tables with new ID's.  These are new tables.
    ############################################################

    SELECT 'UPDATE user_products TABLE WITH NEW PRODUCT_ID';
    # Update user mappings.  Fails FK checks.
    INSERT INTO user_products (`id`, `user_id`, `product_id`, `creator_id`, `created_at`, `updater_id`, `updated_at`)
    SELECT `id`, `Rep_ID`, `Product_ID`, COALESCE(`Created_By`, @CREATOR_ID), COALESCE(`Created_Date`, NOW()), @UPDATER_ID, NOW() FROM rep_product;

    UPDATE user_products t1 JOIN products_to_product t2 ON t1.product_id = t2.old_id
    SET t1.product_id = t2.new_id;

    # Fails FK checks
    SELECT 'UPDATE appointment_products TABLE WITH NEW PRODUCT_ID';
    INSERT INTO appointment_products (`id`, `appointment_id`, `product_id`, `created_at`, `creator_id`, `updater_id`, `updated_at`)
    SELECT `id`, `appointment_id`, `product_id`,
           COALESCE(`created_at`, NOW()), COALESCE(`creator_id`, @CREATOR_ID), @UPDATER_ID, NOW() FROM appointment_product;

    UPDATE appointment_products t1 JOIN products_to_product t2 ON t1.product_id = t2.old_id
    SET t1.product_id = t2.new_id;

    ####################################################################
    # Populate existing tables with new ID's.  These  are re-used tables
    ####################################################################

    SELECT 'UPDATE company_import_product_mappings TABLE WITH NEW PRODUCT_ID';
    INSERT INTO company_import_product_mappings (`id`, `company_id`, `import_client_product_id`, `target_product_id`,
                `creator_id`, `updater_id`, `created_at`, `updated_at`)
    SELECT t1.`id`, `company_id`, `import_client_product_id`, COALESCE(t2.new_id, `target_product_id`),
           COALESCE(`creator_id`, @CREATOR_ID), @UPDATER_ID, COALESCE(`created_at`, NOW()), NOW()
    FROM deprecated_company_import_product_mappings t1
    LEFT JOIN products_to_product t2 ON t1.target_product_id = t2.old_id
    ORDER BY t1.id DESC;


    SELECT 'UPDATE company_products TABLE WITH NEW PRODUCT_ID';
    INSERT INTO company_products (`id`, `company_id`, `company_division_id`, `product_id`, `is_default`, `creator_id`,
                                  `updater_id`, `created_at`, `updated_at`)
    SELECT t1.`id`, `company_id`, `company_division_id`, COALESCE(t2.`new_id`, t1.`product_id`), `is_default`,
           COALESCE(`creator_id`, @CREATOR_ID), @UPDATER_ID, COALESCE(`created_at`, NOW()), NOW()
    FROM deprecated_company_products t1
    LEFT JOIN products_to_product t2 ON t1.product_id = t2.old_id
    ORDER BY t1.id DESC;


    # Fails FK constraints
    SELECT 'UPDATE sample_request_products TABLE WITH NEW PRODUCT_ID';
    INSERT INTO sample_request_products (`id`, `sample_request_id`, `product_id`, `creator_id`, `updater_id`,
                                         `created_at`, `updated_at`)
    SELECT t1.`id`, `sample_request_id`, COALESCE(t2.`new_id`, t1.`product_id`),
           COALESCE(`creator_id`, @CREATOR_ID), @UPDATER_ID, COALESCE(`created_at`, NOW()), NOW()
    FROM deprecated_sample_request_products t1
    LEFT JOIN products_to_product t2 ON t1.product_id = t2.old_id
    ORDER BY t1.id DESC;

    # Fails FK constraints, there are appointments that don't exist.
    SELECT 'UPDATE snapshot_appointment_user_products TABLE WITH NEW PRODUCT_ID';
    INSERT INTO snapshot_appointment_user_products (`id`, `appointment_id`, `user_id`, `product_id`, `type`,
                         `creator_id`, `updater_id`, `created_at`, `updated_at`)
    SELECT t1.`id`, `appointment_id`, `user_id`, COALESCE(t2.`new_id`, t1.`product_id`), `type`,
           @CREATOR_ID, @UPDATER_ID, COALESCE(`created_at`, NOW()), NOW()
    FROM deprecated_snapshot_appointment_user_products t1
    LEFT JOIN products_to_product t2 ON t1.product_id = t2.old_id;
    
    SET FOREIGN_KEY_CHECKS = 1;

#     # Cleanup
#     DROP TABLE tmp_products_to_product;
#     DROP TABLE tmp_products;
#     DROP TABLE tmp_product_cleaned;
#     DROP TABLE tmp_products_cleaned;
#     DROP TABLE tmp_products_unknown;
#     DROP TABLE tmp_user_products;
END;

