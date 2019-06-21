create procedure sp_prep_drugs()
BEGIN

    SET SQL_SAFE_UPDATES = 0;
    SET @CREATOR_ID = 1;
    SET @UPDATER_ID = 1;
    SET @APPROVER_ID = null;
    SET @DELETER_ID = null;
    SET @UKNOWN_CATEGORY_ID = 5;
    SET @DRUG_CATEGORY_ID = 1;
    SET @VACCINE_CATEGORY_ID = 8;

    SET @maxlen_generic_name = (SELECT character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = database() AND table_name = 'products' AND column_name = 'generic_name');


	DROP TABLE IF EXISTS tmp_products_toCompare;
    CREATE TABLE tmp_products_toCompare like products;
    # Make a copy of the products table
    INSERT INTO tmp_products_toCompare select * from products;

    DROP TABLE IF EXISTS tmp_products;
	CREATE TABLE tmp_products like products;

    select 'START UNION';
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

    SELECT 'UNION DONE';

    # Create a copy of this table to use with the synonyms
    DROP TABLE IF EXISTS tmp_products2;
    CREATE TABLE tmp_products2 LIKE tmp_products;
    INSERT INTO tmp_products2 SELECT * FROM tmp_products;

    # Delete all but the new records
    DELETE t1 FROM tmp_products t1 JOIN tmp_products_toCompare t2
		   ON t1.rxcui_id = t2.rxcui_id;

	# Anything with "vaccine", put in the VACCINE category
    UPDATE tmp_products
	SET product_category_id = @VACCINE_CATEGORY_ID
	WHERE name LIKE '%vaccine%' or generic_name LIKE '%vaccine%';

    # Insert new records into the products table
    INSERT INTO products (`name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `is_generic`, `approver_id`, `creator_id`, `deleter_id`, `updater_id`, `created_at`, `updated_at`)
    SELECT DISTINCT `name`, `generic_name`, `description`, `rxcui_id`, `source`, `product_category_id`, `is_generic`, `approver_id`, `creator_id`, `deleter_id`, `updater_id`,`created_at`, `updated_at` from tmp_products;


    # Make a copy of the synonyms
    DROP TABLE IF EXISTS tmp_product_synonyms_toCompare;
    CREATE TABLE tmp_product_synonyms_toCompare like product_synonyms;
    INSERT INTO tmp_product_synonyms_toCompare SELECT * FROM product_synonyms;

    DROP TABLE IF EXISTS tmp_product_synonyms;
    CREATE TABLE tmp_product_synonyms like product_synonyms;

    SELECT 'INSERT INTO TMP_PRODUCT_SYNONYMS';

    INSERT INTO tmp_product_synonyms (`rxcui_id`, `synonym`, `creator_id`, `created_at`)
    SELECT brand_rxcui, STR, @CREATOR_ID, NOW() from stage_rxnconso t3 JOIN (
	SELECT name,t1.rxcui_id as brand_rxcui,t2.rxcui2 as generic_rxcui FROM tmp_products2 t1 JOIN stage_rxnrel t2 on t1.rxcui_id = t2.rxcui1 WHERE (RELA='precise_ingredient_of' or RELA='has_tradename') and is_generic = 0
	) t4 on t3.rxcui = t4.generic_rxcui WHERE (TTY='SY' or TTY='TMSY' or TTY='PIN' or TTY='IN') and  SAB='RXNORM';

    SELECT 'DELETE FROM SYNONYMS';
    DELETE t1 FROM tmp_product_synonyms t1 JOIN tmp_product_synonyms_toCompare t2
		   ON t1.rxcui_id = t2.rxcui_id;

    SELECT 'INSERT NEW SYNONYMS';
	# Insert new records into the product_synonyms table
    INSERT INTO product_synonyms (`rxcui_id`, `synonym`, `creator_id`, `created_at`)
    SELECT `rxcui_id`, `synonym`, `creator_id`, NOW() from tmp_product_synonyms;


END;

