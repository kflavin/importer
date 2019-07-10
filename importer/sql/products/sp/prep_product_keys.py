SP_PREP_PRODUCTKEYS = """
CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`{procedure_name}`()
BEGIN

            
           
            #Prepare ProductKeys based on records which exist in stage_drugVocabulary
            
            SET SESSION optimizer_switch='block_nested_loop=off';
    
    /*--------------------------------------------------------------------------------
        Creae tmp_druglist table using stage_indications and stage_drugVocabulary
        vw_DrugList :: combination of stage_indications and stage_drugVocabulary
    --------------------------------------------------------------------------------*/            
                        
            DROP TABLE IF EXISTS {database}.tmp_druglist;
            
            CREATE TABLE tmp_druglist(
                Drug_ID  VARCHAR(50),
                Drug_Name  VARCHAR(100)
            );
            
            CREATE INDEX idx_tmp_drug_name ON {database}.tmp_druglist (drug_name(100));

            INSERT INTO tmp_druglist 
            SELECT Drug_ID,Drug_Name FROM {database}.vw_DrugList;
            
    /*--------------------------------------------------------------------------------
        Creae unique productList using stage_ndc_product
    --------------------------------------------------------------------------------*/            
            
            DROP TABLE IF EXISTS {database}.tmp_product_list;
            
            CREATE TABLE tmp_product_list (
              ProprietaryName varchar(226) DEFAULT NULL,
              NonProprietaryName varchar(512) DEFAULT NULL
            );
            
            DROP TABLE IF EXISTS {database}.tmp_product_list_new;
            
            CREATE TABLE tmp_product_list_new (
              ProprietaryName varchar(226) DEFAULT NULL,
              NonProprietaryName varchar(512) DEFAULT NULL
            );

            # Create unique product list using stage_ndc_prduct table  
            # Unique combination of proprietaryName ,nonproprietaryName
            INSERT INTO tmp_product_list (ProprietaryName ,NonProprietaryName)
            SELECT DISTINCT  
                UPPER(proprietaryName) AS proprietaryName,
                CASE WHEN UPPER(RIGHT(NonProprietaryName,1)) = ',' THEN 
                    UPPER(SUBSTRING(NonProprietaryName,1,CHAR_LENGTH(NonProprietaryName) - 1)) 
                ELSE 
                    UPPER(NonProprietaryName) 
                END AS NonProprietaryName 
            FROM stage_ndc_product; #48,972
            
    /*---------------------------------------------------------------------------------
        Create tmp_product_keys table using product_keys table
      ---------------------------------------------------------------------------------*/
            
            DROP TABLE IF EXISTS {database}.tmp_product_keys;
            
            CREATE TABLE {database}.tmp_product_keys(
                    Product_ID varchar(50),        
                    Product_Name varchar(226) DEFAULT NULL,  
                    ProprietaryName varchar(226) DEFAULT NULL,
                    NonProprietaryName varchar(512) DEFAULT NULL
            ) ;
                   
            INSERT INTO {database}.tmp_product_keys(Product_ID,Product_Name,ProprietaryName,NonProprietaryName)
            SELECT 
                    DISTINCT   
                    d.Drug_ID as Product_ID,
                    d.Drug_Name as Product_Name,
                    ProprietaryName, 
                    NonProprietaryName     
            FROM 
                    {database}.tmp_product_list n LEFT JOIN {database}.tmp_druglist d ON
                    (n.ProprietaryName = drug_name)
            
            UNION 
            
            SELECT 
                    DISTINCT   
                    d.Drug_ID as Product_ID,                    
                    d.Drug_Name as Product_Name,
                    ProprietaryName, 
                    NonProprietaryName
            FROM 
                    {database}.tmp_product_list n LEFT JOIN {database}.tmp_druglist d on
                    (n.nonProprietaryName = drug_name);
            
            DROP TABLE IF EXISTS {database}.tmp_product_distinct;
            
            CREATE TABLE {database}.tmp_product_distinct(
                    Product_ID varchar(50),  
                    Product_Name varchar(226) DEFAULT NULL,  
                    ProprietaryName varchar(226) DEFAULT NULL,
                    NonProprietaryName varchar(512) DEFAULT NULL
            ) ;

            INSERT INTO tmp_product_distinct
            SELECT DISTINCT product_ID,Product_Name,ProprietaryName,NonProprietaryName 
            FROM tmp_product_keys  WHERE IFNULL(Product_Name,'') <> '';
            #and NonProprietaryName ='GLYCERIN' order by ProprietaryName;         

            CREATE INDEX tmp_product_keys_1 ON {database}.tmp_product_keys (ProprietaryName(226),nonProprietaryName(512));
            CREATE INDEX tmp_product_distinct_1 ON {database}.tmp_product_distinct (ProprietaryName(226),nonProprietaryName(512));
            
            UPDATE tmp_product_keys p 
            JOIN tmp_product_distinct t
            ON 
                    p.ProprietaryName   = t.ProprietaryName AND 
                    p.nonProprietaryName  = t.nonProprietaryName
            SET 
                    p.product_ID  = t.product_ID , 
                    p.product_Name  = t.product_Name ;
            
            DROP TABLE IF EXISTS {database}.tmp_product_keys_final;
            
            CREATE TABLE {database}.tmp_product_keys_final(
                    Product_ID varchar(50),
                    Product_Type varchar(50),
                    Product_Name varchar(226) DEFAULT NULL,  
                    Product_Code varchar(10) DEFAULT NULL,
                    ProprietaryName varchar(226) DEFAULT NULL,
                    NonProprietaryName varchar(512) DEFAULT NULL
            ) ;
            
            INSERT INTO {database}.tmp_product_keys_final(Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName)
            SELECT distinct Product_ID,'DRUGS' as Product_Type,Product_Name,ProprietaryName,NonProprietaryName FROM tmp_product_keys;
            
            CREATE TABLE {database}.tmp_product_keys_final_toCompare(
                    Product_ID varchar(50),
                    Product_Type varchar(50),
                    Product_Name varchar(226) DEFAULT NULL,  
                    Product_Code varchar(10) DEFAULT NULL,
                    ProprietaryName varchar(226) DEFAULT NULL,
                    NonProprietaryName varchar(512) DEFAULT NULL
            ) ;
            
            INSERT INTO {database}.tmp_product_keys_final_toCompare (Product_ID,Product_Type,Product_Name,Product_Code,ProprietaryName,NonProprietaryName)
            SELECT Product_ID,Product_Type,Product_Name,Product_Code,ProprietaryName,NonProprietaryName
            FROM {database}.product_keys where Product_Type = 'DRUGS';
            
            #select * from tmp_product_keys_final where ProprietaryName = 'Headache' and nonproprietaryName = 'CIMICIFUGA RACEMOSA, GELSEMIUM SEMPERVIRENS, RHUS TOX';
    /*----------------------------------------------------------------------------------------- 
        Delete products which already exists in product_keys table, only keep new products
      -----------------------------------------------------------------------------------------*/
            
            DELETE t.* FROM 
               tmp_product_keys_final t JOIN 
               tmp_product_keys_final_toCompare k 
               ON 
                   t.ProprietaryName = k.ProprietaryName AND 
                   t.NonProprietaryName = k.NonProprietaryName ; 
            
            # Insert only new records
            INSERT INTO {database}.product_keys(Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName,Eff_Date,End_Eff_Date,Created_By)
            SELECT DISTINCT Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName,curdate(),NULL,0 
            FROM tmp_product_keys_final;
    
    /*--------------------------------------------------------------------------------------------
        Prepare ProductKeys based on records which doesnot exist directly in stage_drugVocabulary  
      --------------------------------------------------------------------------------------------*/
    
            #make a copy
            INSERT INTO tmp_product_list_new SELECT * FROM tmp_product_list; 
            
            DELETE t.* FROM 
            tmp_product_list  t JOIN 
            product_keys  k ON 
                t.proprietaryName   = k.proprietaryName AND 
                t.nonproprietaryName  = k.nonproprietaryName;
                        
            DROP TABLE IF EXISTS  {database}.tmp_productSynonyms;

            CREATE TABLE {database}.tmp_productSynonyms(
                proprietaryName varchar(226),
                nonproprietaryName  varchar(512),
                synonym  varchar(512)
            ) ;

            INSERT INTO {database}.tmp_productSynonyms
            SELECT DISTINCT upper(n.proprietaryName) as prop,upper(n.nonproprietaryName) as nonprop,upper(t.nonproprietaryName) as synonym
            FROM tmp_product_list  t 
            JOIN tmp_product_list_new n 
            ON  
                    t.proprietaryName   = n.proprietaryName AND 
                    t.nonproprietaryName <> n.nonproprietaryName
            UNION
            
            SELECT DISTINCT upper(n.proprietaryName) as prop,upper(n.nonproprietaryName) as nonprop,upper(t.proprietaryName) as synonym
            FROM tmp_product_list  t 
            JOIN tmp_product_list_new n 
            ON 
                    t.nonproprietaryName = n.nonproprietaryName AND 
                    t.proprietaryName <> n.proprietaryName ; 
                        

    /*--------------------------------------------------------------------------------------------
        Insert 
      --------------------------------------------------------------------------------------------*/
            SET SESSION optimizer_switch='block_nested_loop=off';
            
            TRUNCATE TABLE {database}.tmp_product_keys_final;            
            
            INSERT INTO  {database}.tmp_product_keys_final(Product_Name,Product_ID,ProprietaryName,NonProprietaryName)
            SELECT DISTINCT v.Product_Name,Product_ID,x.proprietaryName,synonym
            FROM tmp_productSynonyms x
            JOIN product_keys v 
            ON   x.proprietaryName   = v.proprietaryName or x.nonproprietaryName  = v.nonproprietaryName;
            
            
            DELETE t.* FROM 
            tmp_product_keys_final     t JOIN 
            tmp_product_keys_final_toCompare k ON 
                t.proprietaryName   = k.proprietaryName AND 
                t.nonproprietaryName  = k.nonproprietaryName;
            
            INSERT INTO  {database}.product_keys(Product_Name,Product_ID,Product_Type,ProprietaryName,NonProprietaryName ,Eff_Date,End_Eff_Date,Created_By)
            SELECT DISTINCT Product_Name,Product_ID,'DRUGS' as Product_Type,ProprietaryName,NonProprietaryName,curdate(),NULL ,0 from tmp_product_keys_final;
            
            #--------------------------------------------------------------------------------------------*/
            # MEDICAL DEVICES
            #--------------------------------------------------------------------------------------------*/
            
            INSERT INTO {database}.tmp_product_keys_final (Product_ID,Product_Name,Product_Code,ProprietaryName,NonProprietaryName)
            SELECT 
                DISTINCT PrimaryDI as Product_ID,ProductCodeName as Product_Name,c.ProductCode as Product_Code,UPPER(DeviceDescription) as ProprietaryName,UPPER(DeviceDescription) as NonProprietaryName
                #d.PrimaryDI as ProprietaryID,    
            FROM {database}.stage_fda_device  d  
            JOIN rxvantage_talend.tmp_product_codes c ON d.primaryDI = c.PrimaryDI 
            WHERE IFNULL(DeviceDescription,'') <> '';


            TRUNCATE TABLE {database}.tmp_product_keys_final_toCompare;
            
            INSERT INTO {database}.tmp_product_keys_final_toCompare (Product_ID,Product_Type,Product_Name,Product_Code,ProprietaryName,NonProprietaryName)
            SELECT Product_ID,Product_Type,Product_Name,Product_Code,ProprietaryName,NonProprietaryName
            FROM {database}.product_keys where Product_Type = 'MEDICAL DEVICES';
           
            DELETE t.* FROM 
            tmp_product_keys_final     t JOIN 
            tmp_product_keys_final_toCompare k ON 
                t.proprietaryName   = k.proprietaryName  AND 
                t.nonproprietaryName  = k.nonproprietaryName  AND
                t.Product_Code    = k.Product_Code;
            
            INSERT INTO  {database}.product_keys(Product_Name,Product_ID,Product_Type,ProprietaryName,NonProprietaryName ,Eff_Date,End_Eff_Date,Created_By)
            SELECT DISTINCT Product_Name,Product_ID,'MEDICAL DEVICES' as Product_Type,ProprietaryName,NonProprietaryName,curdate(),NULL ,0 from tmp_product_keys_final;
            
            /*UNION
            SELECT DISTINCT v.Product_Name,Product_ID,'DRUGS',x.proprietaryName,synonym,curdate(),NULL ,0 
            FROM tmp_productSynonyms x
            JOIN product_keys v 
            ON  x.nonproprietaryName = v.nonproprietaryName ;
            */
            
            #Clean up            
            DROP TABLE IF EXISTS {database}.tmp_druglist;
            DROP TABLE IF EXISTS {database}.tmp_product_keys;
            DROP TABLE IF EXISTS {database}.tmp_product_distinct;
            DROP TABLE IF EXISTS {database}.tmp_product_keys_final;
            DROP TABLE IF EXISTS {database}.tmp_product_list;
            DROP TABLE IF EXISTS {database}.tmp_product_list_new;
            DROP TABLE IF EXISTS {database}.tmp_productSynonyms;
            DROP TABLE IF EXISTS {database}.tmp_product_keys_final_toCompare;
END
"""