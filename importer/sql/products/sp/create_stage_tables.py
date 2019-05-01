SP_CREATE_STAGINGTABLES = """
CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`{procedure_name}`()
BEGIN

########################################################################
# STAGING TABLES
########################################################################

		### STAGE_NDC_PRODUCT ################################################################################################

			DROP TABLE IF EXISTS {database}.stage_ndc_product;

			CREATE TABLE {database}.stage_ndc_product (
				ProductID 							VARCHAR(100) NOT NULL,
				ProductNDC 							VARCHAR(50) DEFAULT NULL,
				ProductTypeName 					VARCHAR(50) DEFAULT NULL,
				ProprietaryName 					VARCHAR(400) NOT NULL,
				ProprietaryNameSuffix 				VARCHAR(400) DEFAULT NULL,
				NonProprietaryName 					VARCHAR(600) DEFAULT NULL,
				DosageFormName 						VARCHAR(50) DEFAULT NULL,
				RouteName 							VARCHAR(500) DEFAULT NULL,
				StartMarketingDate 					VARCHAR(20) DEFAULT NULL,
				EndMarketingDate 					VARCHAR(20) DEFAULT NULL,
				Marketingcategory 					VARCHAR(50) DEFAULT NULL,
				ApplicationNumber 					VARCHAR(50) DEFAULT NULL,
				LabelerName 						VARCHAR(500) DEFAULT NULL,
				SubstanceName 						VARCHAR(5000) DEFAULT NULL,
				Active_Numerator_Strength 			VARCHAR(1500) DEFAULT NULL,
				Active_Ingred_Unit 					TEXT,
				Pharm_Classes 						VARCHAR(4000) DEFAULT NULL,
				DeaSchedule 						VARCHAR(20) DEFAULT NULL,
				NDC_Exclude_Flag 					CHAR(1) DEFAULT NULL,
				Listing_Record_Certified_Through	VARCHAR(20) DEFAULT NULL	,		  
				PRIMARY KEY (ProductID)
			);

			#drop index idx_NDC_proprietaryName on {database}.stage_ndc_product;
			CREATE INDEX idx_NDC_proprietaryName ON {database}.stage_ndc_product (proprietaryName(400));
			CREATE INDEX idx_NDC_nonproprietaryName ON {database}.stage_ndc_product (nonproprietaryName(512));

		### STAGE_ORANGEBOOK_PRODUCT ########################################################################################

			DROP TABLE IF EXISTS {database}.stage_orangebook_product;
		
			CREATE TABLE {database}.stage_orangebook_product (
					Ingredient 			VARCHAR(500) DEFAULT NULL,
					DF_Route 			VARCHAR(500) DEFAULT NULL,
					Trade_Name 			VARCHAR(400) NOT NULL,
					Applicant 			VARCHAR(50) DEFAULT NULL,
					Strength 			VARCHAR(500) DEFAULT NULL,
					Appl_Type 			VARCHAR(1) DEFAULT NULL,
					Appl_No 			INT(11) NOT NULL,
					Product_No 			INT(11) NOT NULL,
					TE_Code 			VARCHAR(20) DEFAULT NULL,
					Approval_Date 		VARCHAR(100) DEFAULT NULL,
					RLD 				CHAR(10) DEFAULT NULL,
					RS 					VARCHAR(20) DEFAULT NULL,
					Type 				VARCHAR(10) DEFAULT NULL,
					Applicant_Full_Name VARCHAR(500) DEFAULT NULL
					#KEY idx_OB_product(Product_No,Appl_No,Trade_Name)
				);

			#drop index idx_OB_product_ProprietaryName on {database}.stage_orangebook_product
			CREATE INDEX idx_OB_product_ProprietaryName ON {database}.stage_orangebook_product (Trade_Name(400));

		### STAGE_FDA_DEVICE ################################################################################################

			DROP TABLE IF EXISTS {database}.stage_fda_device;
            
            CREATE TABLE {database}.stage_fda_device (
				PrimaryDI 						VARCHAR(200),
				PublicDeviceRecordKey  			VARCHAR(50),
				PublicVersionStatus  			VARCHAR(200),
				DeviceRecordStatus  			VARCHAR(200),
				PublicVersionNumber 			VARCHAR(50),
				PublicVersionDate  				VARCHAR(20),
				DevicePublishDate  				VARCHAR(20),
				DeviceCommDistributionEndDate  	VARCHAR(20),
				DeviceCommDistributionStatus   	VARCHAR(2000),         
				BrandName   					VARCHAR(100),
				VersionModelNumber   			VARCHAR(100),
				CatalogNumber   				VARCHAR(100),
				DunsNumber 						VARCHAR(100),
				CompanyName   					VARCHAR(500),
				DeviceCount 					VARCHAR(200),
				DeviceDescription   			VARCHAR(3000),
				DMExempt   						VARCHAR(1100),
				PremarketExempt  				VARCHAR(1500),
				DeviceHCTP  					VARCHAR(1000),
				DeviceKit  						VARCHAR(200),
				DeviceCombinationProduct  		VARCHAR(250),
				SingleUse  						VARCHAR(500),
				LotBatch 						VARCHAR(50),
				SerialNumber   					VARCHAR(250),
				ManufacturingDate  				VARCHAR(20),
				ExpirationDate  				VARCHAR(20),
				DonationIdNumber  				VARCHAR(50),
				LabeledContainsNRL  			VARCHAR(50),
				LabeledNoNRL   					VARCHAR(50),
				MRISafetyStatus   				VARCHAR(1000),
				RX   							VARCHAR(50),
				OTC  							VARCHAR(50),
				DeviceSterile 					VARCHAR(50),
				SterilizationPriorToUse 		VARCHAR(100),
                primary key (PrimaryDI)
		);
        
        #Select Count(*),PrimaryDI from {database}.stage_fda_device group by PrimaryDI having count(*) > 1;

		### STAGE_FDA_IDENTIFIERS #############################################
		
			DROP TABLE IF EXISTS {database}.stage_fda_identifiers;
			
			CREATE TABLE {database}.stage_fda_identifiers (
				PrimaryDI 				VARCHAR(200),
				DeviceID 				VARCHAR(200),
				DeviceIdType 			VARCHAR(50),
				DeviceIdIssuingAgency 	VARCHAR(200),
				ContainsDINumber 		VARCHAR(50),
				PkgQuantity 			VARCHAR(50),
				PkgDiscontinueDate 		VARCHAR(50),
				PkgStatus 				VARCHAR(50),
				PkgType 				VARCHAR(50)
			);
			
            CREATE INDEX idx_stage_fda_Contacts_PrimaryDI ON {database}.stage_fda_identifiers (PrimaryDI(200));
        
        
        ### STAGE_FDA_CONTACTS ################################################
       
			DROP TABLE IF EXISTS {database}.stage_fda_contacts;
		   
			CREATE TABLE {database}.stage_fda_contacts (
				PrimaryDI 		VARCHAR(200),
				Phone 			VARCHAR(20),
				PhoneExtension 	VARCHAR(50), 
				Email 			VARCHAR(500)                
			);

			CREATE INDEX idx_stage_fda_Contacts_PrimaryDI ON {database}.stage_fda_contacts (PrimaryDI(200));
            
		### STAGE_MARKETINGCODES #############################################
       
			DROP TABLE IF EXISTS {database}.stage_marketingcodes;
		
			CREATE TABLE {database}.stage_marketingcodes (
				SplAcceptableTerm varchar(400),
				Code varchar(10)
            );

		### STAGE_INDICATIONS ###############################################
		
			DROP TABLE IF EXISTS {database}.stage_indications;
			
			CREATE TABLE {database}.stage_indications (
					  Drug_Name 		VARCHAR(100) DEFAULT NULL,
					  Drug_ID 			VARCHAR(50) DEFAULT NULL,
					  Ind_Name 			VARCHAR(500) DEFAULT NULL,
					  Ind_ID 			VARCHAR(50) DEFAULT NULL,
					  NCT 				VARCHAR(50) DEFAULT NULL,
					  Status 			VARCHAR(50) DEFAULT NULL,
					  Phase 			VARCHAR(50) DEFAULT NULL,
					  DetailedStatus 	VARCHAR(500) DEFAULT NULL
			);
			
			CREATE INDEX idx_Stage_Ind_Drug_Name ON {database}.stage_indications (Drug_Name(100));
            CREATE INDEX idx_Stage_Drug_ID ON {database}.stage_indications (Drug_ID);

		### STAGE_DRUGVOCABULARY ###############################################
				
			DROP TABLE IF EXISTS {database}.stage_drugVocabulary;

			CREATE TABLE {database}.stage_drugVocabulary
			(
				 Drug_ID varchar(50),
				 Drug_Name  varchar(100),
				 Accession_Numbers  varchar(500),
				 CAS  varchar(100),
				 UNII varchar(100),
				 Synonyms varchar(500),
				 Standard_InChI_Key varchar(500)
			);
            
            ### STAGE_CMS_SERVICE ###############################################
            
            DROP TABLE IF EXISTS {database}.`stage_cms_service`;
            
            CREATE TABLE IF NOT EXISTS {database}.`stage_cms_service` (
				`cpt_code` VARCHAR(10) DEFAULT NULL,
				`service` VARCHAR(184) DEFAULT NULL
			);
            
            ### STAGE_FDA_GMDN ###############################################
            
            DROP TABLE IF EXISTS {database}.`stage_fda_gmdn`;
            
            CREATE TABLE {database}.`stage_fda_gmdn` (
			  `PrimaryDI` varchar(30) DEFAULT NULL,
			  `gmdnPTName` varchar(128) DEFAULT NULL,
			  `gmdnPTDefinition` varchar(1024) DEFAULT NULL,
			  KEY `idx_tmp_gmdn_PrimaryDI` (`PrimaryDI`),
			  KEY `idx_tmp_gmdn_gmdnPTName` (`gmdnPTName`)
			);
            
            ### STAGE_FDA_GMDN ###############################################
            
            DROP TABLE IF EXISTS {database}.`stage_fda_productcodes`;
            
            CREATE TABLE {database}.`stage_fda_productcodes` (
			  `PrimaryDI` varchar(30) DEFAULT NULL,
			  `productCode` varchar(3) DEFAULT NULL,
			  `productCodeName` varchar(128) DEFAULT NULL,
			  KEY `idx_tmp_product_codes_PrimaryDI` (`PrimaryDI`),
			  KEY `idx_tmp_product_codes_productCode_productCodeName` (`productCode`,`productCodeName`),
			  KEY `idx_tmp_product_codes_productCodeName` (`productCodeName`),
			  KEY `idx_tmp_product_codes_productcode` (`productCode`)
			);
		
END
"""