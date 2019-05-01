SP_PREP_DRUGMASTER = """
CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`{procedure_name}`()
BEGIN
			SELECT @max_dtEffective := MAX(Eff_Date) FROM {database}.drug_master WHERE end_eff_date IS NULL;

            DROP TABLE IF EXISTS {database}.tmp_product_keys;
            
			CREATE TABLE {database}.tmp_product_keys (
				    ProductKey_ID mediumint(9) NOT NULL ,
				    Product_ID varchar(50),
				    Product_Type varchar(50),
				    Product_Name varchar(226) DEFAULT NULL,  
				    ProprietaryName varchar(226) DEFAULT NULL,
				    NonProprietaryName varchar(512) DEFAULT NULL
			) ;
			
            INSERT INTO tmp_product_keys (ProductKey_ID,Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName)
            SELECT ProductKey_ID,Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName 
            FROM {database}.product_keys 
            WHERE IFNULL(PRODUCT_ID,'') <> '' and Eff_Date > @max_dtEffective and Product_Type = 'DRUGS' ;
            
            DROP TABLE IF EXISTS tmp_drug_master_tocompare;
           
            CREATE TABLE tmp_drug_master_tocompare (
				  ProductKey_ID mediumint(9) DEFAULT NULL,
				  Client_Product_ID varchar(255) DEFAULT NULL,
				  LabelerName varchar(121) DEFAULT NULL,
				  ProductNDC varchar(10) DEFAULT NULL,
				  ProprietaryName varchar(226) DEFAULT NULL,
				  NonProprietaryName varchar(512) DEFAULT NULL,
				  ProductTypeName varchar(27) DEFAULT NULL,
				  Source_Type enum('FDA','USER','FDA_DISAPPROVED') DEFAULT NULL,
				  Company_ID varchar(12) DEFAULT NULL,
				  Marketing_Cat_Name varchar(40) DEFAULT NULL,
				  Marketing_Cat_Name_Desc varchar(40) DEFAULT NULL,
				  TE_Code varchar(50) DEFAULT NULL,
				  Interpretation varchar(255) DEFAULT NULL,
				  TE_Type varchar(50) DEFAULT NULL,
				  NDC_Exclude_Flag varchar(1) DEFAULT NULL,
				  Ind_Drug_ID varchar(8) DEFAULT NULL,
				  Ind_Drug_Name varchar(50) DEFAULT NULL,
				  Ind_Name varchar(255) DEFAULT NULL,
				  Ind_Status varchar(10) DEFAULT NULL,
				  Ind_NCT varchar(50) DEFAULT NULL,
				  Ind_Phase varchar(50) DEFAULT NULL,
				  Ind_DetailedStatus varchar(500) DEFAULT NULL
			  );
        
            #CREATE INDEX idx_drug_master_Drug_ID ON {database}.drug_master (Ind_Drug_ID);
            CREATE INDEX idx_tmp_product_keys_Product_ID ON {database}.tmp_product_keys (Product_ID);

			 
            INSERT INTO tmp_drug_master_tocompare
            SELECT distinct
					d.ProductKey_ID,d.Client_Product_ID,d.LabelerName,d.ProductNDC,d.ProprietaryName,d.NonProprietaryName,
					d.ProductTypeName, d.Source_Type,d.Company_ID, d.Marketing_Cat_Name, d.Marketing_Cat_Name_Desc, 
                    d.TE_Code,d.Interpretation,d.TE_Type, d.NDC_Exclude_Flag,d.Ind_Drug_ID,d.Ind_Drug_Name,Ind_Name,d.Ind_Status,
                    d.Ind_NCT,d.Ind_Phase,d.Ind_DetailedStatus
			FROM drug_master d join tmp_product_keys t on d.Ind_Drug_ID = t.product_id;
			
            #SELECT * FROM tmp_drug_master_tocompare limit 100;
            
            /*---------------------------------------------------------------------------------
				Prepare NDCProduct
			  ------------------------------------------------------------------------------- */
	
            DROP TABLE IF EXISTS {database}.tmp_NDCProduct;
            
			CREATE TABLE tmp_NDCProduct(
				Client_Product_ID varchar(255),
				LabelerName varchar(500),
				ProductNDC varchar(50),
                ProductTypeName varchar(50),
				Marketing_Cat_Name varchar(50),
				Marketing_Cat_Name_Desc varchar(50),
                NDC_Exclude_Flag char(1),
				ProprietaryName varchar(400),
				NonProprietaryName varchar(600)
			);

			Insert into {database}.tmp_NDCProduct
			SELECT DISTINCT
				ProductID,
				LabelerName,
				ProductNDC,
                ProductTypeName,
				MarketingCategory as Marketing_Cat_Name,
				ApplicationNumber as Marketing_Cat_Name_Desc ,
                NDC_Exclude_Flag,
				UPPER(ProprietaryName) AS ProprietaryName,
				CASE WHEN UPPER(RIGHT(NonProprietaryName,1)) = ',' THEN 
					UPPER(SUBSTRING(NonProprietaryName,1,CHAR_LENGTH(NonProprietaryName) - 1)) 
				ELSE 
					UPPER(NonProprietaryName) 
				END AS NonProprietaryName 
			FROM {database}.stage_ndc_product;

			/*---------------------------------------------------------------------------------
				Prepare tmp_stage_orangebook_product by getting TE_Code and TE_Type
			  ------------------------------------------------------------------------------- */
	
			DROP TABLE IF EXISTS {database}.tmp_stage_orangebook_product;
			
			#Create Temp Table    
			CREATE TABLE {database}.tmp_stage_orangebook_product(	
					ProductKey_ID 			MEDIUMINT(9),
                    Product_ID	 			VARCHAR(50),
                    Product_Name			VARCHAR(400),
					ProprietaryName		 	VARCHAR(400),
					NonProprietaryName  	VARCHAR(512),
					TE_Code 				VARCHAR(20),
					TE_Type					ENUM('RX','OTC','DISCN')
			);                                                                                                                                                        
			
			CREATE INDEX idx_tmp_1_OB_ProprietaryName ON {database}.tmp_stage_orangebook_product (ProprietaryName);
		
			INSERT INTO {database}.tmp_stage_orangebook_product(
					ProductKey_ID,Product_ID,Product_Name,ProprietaryName,NonProprietaryName,TE_Code,TE_Type)		
			SELECT 
					DISTINCT k.ProductKey_ID,k.Product_ID,k.Product_Name,k.ProprietaryName ,k.NonProprietaryName,TE_code ,Type
			FROM 
					{database}.tmp_product_keys k 
			LEFT JOIN 
					{database}.stage_orangebook_product o ON trade_name = ProprietaryName 
			;
            
			/*---------------------------------------------------------------------------------
				Prepare tmp_stage_orangebook_NDC_product by getting TE_Code and TE_Type
			  ------------------------------------------------------------------------------- */
	
            DROP TABLE IF EXISTS {database}.tmp_stage_orangebook_Ndc_product;
            
            CREATE TABLE {database}.tmp_stage_orangebook_Ndc_product(	
					ProductKey_ID 			MEDIUMINT(9),
                    Product_ID 				VARCHAR(50),
                    Client_Product_ID		VARCHAR(50),
                    ProductName				VARCHAR(400),
					ProprietaryName 		VARCHAR(400),
					NonProprietaryName  	VARCHAR(512),                    
					TE_Code 				VARCHAR(20),
					TE_Type					enum('RX','OTC','DISCN'),
                    ProductTypeName			VARCHAR(50),
                    LabelerName				VARCHAR(500),     
                    ProductNDC 				VARCHAR(50),     
                    Marketing_Cat_Name  	VARCHAR(50),     
                    Marketing_Cat_Name_Desc VARCHAR(50),     
                    NDC_Exclude_Flag 		char(1)
			);  
            
            CREATE INDEX tmp_NDCProduct ON {database}.tmp_NDCProduct(ProprietaryName,NonProprietaryName);
            
            CREATE INDEX idx_tmp_2_OB_Prop_nonprop ON {database}.tmp_stage_orangebook_Ndc_product (ProprietaryName,NonProprietaryName);
		    
            INSERT INTO {database}.tmp_stage_orangebook_Ndc_product (ProductKey_ID,LabelerName,ProductNDC,ProprietaryName,NonProprietaryName,ProductTypeName,
				  Marketing_Cat_Name,Marketing_Cat_Name_Desc,TE_Code,TE_Type,NDC_Exclude_Flag,Product_ID,Client_Product_ID)
            SELECT ProductKey_ID,LabelerName,ProductNDC,k.ProprietaryName,k.NonProprietaryName,ProductTypeName,
				  Marketing_Cat_Name,Marketing_Cat_Name_Desc,k.TE_Code,TE_Type,NDC_Exclude_Flag,Product_ID,Client_Product_ID
            FROM       
						{database}.tmp_stage_orangebook_product k
            LEFT JOIN  
						{database}.tmp_NDCProduct n ON k.ProprietaryName = n.ProprietaryName and k.NonProprietaryName = n.NonProprietaryName;
            
            CREATE INDEX idx_tmp_2_OB_Drug_ID ON {database}.tmp_stage_orangebook_Ndc_product (Product_ID);

			Drop table if exists tmp_drug_master;
            
			#TRUNCATE TABLE {database}.drug_master;
			CREATE TABLE tmp_drug_master (
				  ProductKey_ID mediumint(9) DEFAULT NULL,
				  Client_Product_ID varchar(255) DEFAULT NULL,
				  LabelerName varchar(121) DEFAULT NULL,
				  ProductNDC varchar(10) DEFAULT NULL,
				  ProprietaryName varchar(226) DEFAULT NULL,
				  NonProprietaryName varchar(512) DEFAULT NULL,
				  ProductTypeName varchar(27) DEFAULT NULL,
				  Source_Type enum('FDA','USER','FDA_DISAPPROVED') DEFAULT NULL,
				  Company_ID varchar(12) DEFAULT NULL,
				  Marketing_Cat_Name varchar(40) DEFAULT NULL,
				  Marketing_Cat_Name_Desc varchar(40) DEFAULT NULL,
				  TE_Code varchar(50) DEFAULT NULL,
				  Interpretation varchar(255) DEFAULT NULL,
				  TE_Type varchar(50) DEFAULT NULL,
				  NDC_Exclude_Flag varchar(1) DEFAULT NULL,
				  Ind_Drug_ID varchar(8) DEFAULT NULL,
				  Ind_Drug_Name varchar(50) DEFAULT NULL,
				  Ind_Name varchar(255) DEFAULT NULL,
				  Ind_Status varchar(10) DEFAULT NULL,
				  Ind_NCT varchar(50) DEFAULT NULL,
				  Ind_Phase varchar(50) DEFAULT NULL,
				  Ind_DetailedStatus varchar(500) DEFAULT NULL
			  );

            SET SESSION optimizer_switch='block_nested_loop=off';
			#Insert Into NDC        
			INSERT INTO {database}.tmp_drug_master(
					  ProductKey_ID,Client_Product_ID,LabelerName,ProductNDC,ProprietaryName,NonProprietaryName,ProductTypeName,
					  Marketing_Cat_Name,Marketing_Cat_Name_Desc,TE_Code,TE_Type,NDC_Exclude_Flag,
					  Ind_Drug_Name,Ind_Drug_ID,Ind_Name,Ind_Status,Ind_NCT,Ind_Phase,Ind_DetailedStatus)
			SELECT  
					  DISTINCT ProductKey_ID,Client_Product_ID,LabelerName,ProductNDC,k.ProprietaryName,k.NonProprietaryName,ProductTypeName,
					  Marketing_Cat_Name,Marketing_Cat_Name_Desc,k.TE_Code,TE_Type,NDC_Exclude_Flag,
					  Drug_name as Ind_Drug_Name,i.Drug_ID as Ind_Drug_ID,Ind_Name,Status as Ind_Status,NCT as Ind_NCT,
					  phase as Ind_Phase,DetailedStatus as Ind_DetailedStatus					  
			FROM  
					{database}.tmp_stage_orangebook_Ndc_product k 			   
			LEFT JOIN  
					{database}.stage_indications i ON i.Drug_id = k.Product_ID;
                    
			DELETE t.* FROM 
               {database}.tmp_drug_master t JOIN 
               {database}.tmp_drug_master_tocompare k 
			ON 
				   t.ProprietaryName 	= k.ProprietaryName 	AND 
				   t.NonProprietaryName = k.NonProprietaryName 	AND
                   t.Client_Product_ID	= k.Client_Product_ID  	AND 
                   t.ProductNDC 		= k.ProductNDC 			AND 
                   t.Ind_Drug_ID 		= k.Ind_Drug_ID 		AND 
                   t.Ind_Name 			= k.Ind_Name 			AND
				   t.Ind_Status 		= k.Ind_Status 			AND 
                   t.Ind_Phase 			= k.Ind_Phase 			AND 
                   t.Ind_DetailedStatus = k.Ind_DetailedStatus 	AND 
                   t.TE_CODE 			= k.TE_CODE 			AND 
                   t.TE_TYPE 			= k.TE_TYPE 			AND 
                   t.NDC_Exclude_Flag 	= k.NDC_Exclude_Flag 	AND 
                   t.ind_nct 			= k.ind_nct
                   ; 
            #993,793
            
            INSERT INTO {database}.drug_master(
					  ProductKey_ID,Client_Product_ID,LabelerName,ProductNDC,ProprietaryName,NonProprietaryName,ProductTypeName,
					  Marketing_Cat_Name,Marketing_Cat_Name_Desc,TE_Code,TE_Type,NDC_Exclude_Flag,
					  Ind_Drug_Name,Ind_Drug_ID,Ind_Name,Ind_Status,Ind_NCT,Ind_Phase,Ind_DetailedStatus,
					  Eff_Date,End_Eff_Date,Created_By
					  )
			SELECT DISTINCT ProductKey_ID,Client_Product_ID,LabelerName,ProductNDC,ProprietaryName,NonProprietaryName,ProductTypeName,
					  Marketing_Cat_Name,Marketing_Cat_Name_Desc,TE_Code,TE_Type,NDC_Exclude_Flag,
					  Ind_Drug_Name,Ind_Drug_ID,Ind_Name,Ind_Status,Ind_NCT,Ind_Phase,Ind_DetailedStatus,
					  curdate(),NULL,0
			FROM {database}.tmp_drug_master;
            
            DROP TABLE IF EXISTS {database}.tmp_product_keys;
            DROP TABLE IF EXISTS {database}.tmp_NDCProduct;
            DROP TABLE IF EXISTS {database}.tmp_stage_orangebook_product;
            DROP TABLE IF EXISTS {database}.tmp_stage_orangebook_Ndc_product; 
            DROP TABLE IF EXISTS {database}.tmp_drug_master;
END
"""