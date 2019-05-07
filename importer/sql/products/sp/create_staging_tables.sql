CREATE DEFINER=`{user}`@`%` PROCEDURE `sp_create_staging_tables`()
BEGIN
########################################################################
# STAGING TABLES
########################################################################

		### STAGE_NDC_PRODUCT ################################################################################################

			DROP TABLE IF EXISTS stage_ndc_product;

			CREATE TABLE `stage_ndc_product` (
			`ProductID` varchar(100) NOT NULL,
			`ProductNDC` varchar(50) DEFAULT NULL,
			`ProductTypeName` varchar(50) DEFAULT NULL,
			`ProprietaryName` varchar(400) NOT NULL,
			`ProprietaryNameSuffix` varchar(400) DEFAULT NULL,
			`NonProprietaryName` varchar(600) DEFAULT NULL,
			`DosageFormName` varchar(50) DEFAULT NULL,
			`RouteName` varchar(500) DEFAULT NULL,
			`StartMarketingDate` varchar(20) DEFAULT NULL,
			`EndMarketingDate` varchar(20) DEFAULT NULL,
			`Marketingcategory` varchar(50) DEFAULT NULL,
			`ApplicationNumber` varchar(50) DEFAULT NULL,
			`LabelerName` varchar(500) DEFAULT NULL,
			`SubstanceName` varchar(5000) DEFAULT NULL,
			`Active_Numerator_Strength` varchar(1500) DEFAULT NULL,
			`Active_Ingred_Unit` text,
			`Pharm_Classes` varchar(4000) DEFAULT NULL,
			`DeaSchedule` varchar(20) DEFAULT NULL,
			`NDC_Exclude_Flag` char(1) DEFAULT NULL,
			`Listing_Record_Certified_Through` varchar(20) DEFAULT NULL,
			PRIMARY KEY (`ProductID`),
			KEY `idx_NDC_proprietaryName` (`ProprietaryName`),
			KEY `idx_NDC_nonproprietaryName` (`NonProprietaryName`(512)),
			KEY `stage_ndc_product_1` (`ProprietaryName`(226))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			#drop index idx_NDC_proprietaryName on stage_ndc_product;
			CREATE INDEX idx_NDC_proprietaryName ON stage_ndc_product (proprietaryName(400));
			CREATE INDEX idx_NDC_nonproprietaryName ON stage_ndc_product (nonproprietaryName(512));

		### STAGE_ORANGEBOOK_PRODUCT ########################################################################################

			DROP TABLE IF EXISTS stage_orangebook_product;
		
			CREATE TABLE `stage_orangebook_product` (
			`Ingredient` varchar(500) DEFAULT NULL,
			`DF_Route` varchar(500) DEFAULT NULL,
			`Trade_Name` varchar(400) NOT NULL,
			`Applicant` varchar(50) DEFAULT NULL,
			`Strength` varchar(500) DEFAULT NULL,
			`Appl_Type` varchar(1) DEFAULT NULL,
			`Appl_No` int(11) NOT NULL,
			`Product_No` int(11) NOT NULL,
			`TE_Code` varchar(20) DEFAULT NULL,
			`Approval_Date` varchar(100) DEFAULT NULL,
			`RLD` char(10) DEFAULT NULL,
			`RS` varchar(20) DEFAULT NULL,
			`Type` varchar(10) DEFAULT NULL,
			`Applicant_Full_Name` varchar(500) DEFAULT NULL,
			KEY `idx_OB_product_ProprietaryName` (`Trade_Name`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			#drop index idx_OB_product_ProprietaryName on stage_orangebook_product
			CREATE INDEX idx_OB_product_ProprietaryName ON stage_orangebook_product (Trade_Name(400));

		### STAGE_FDA_DEVICE ################################################################################################

			DROP TABLE IF EXISTS stage_fda_device;
            
            CREATE TABLE `stage_fda_device` (
			`PrimaryDI` varchar(200) NOT NULL,
			`PublicDeviceRecordKey` varchar(50) DEFAULT NULL,
			`PublicVersionStatus` varchar(200) DEFAULT NULL,
			`DeviceRecordStatus` varchar(200) DEFAULT NULL,
			`PublicVersionNumber` varchar(50) DEFAULT NULL,
			`PublicVersionDate` varchar(20) DEFAULT NULL,
			`DevicePublishDate` varchar(20) DEFAULT NULL,
			`DeviceCommDistributionEndDate` varchar(20) DEFAULT NULL,
			`DeviceCommDistributionStatus` varchar(2000) DEFAULT NULL,
			`BrandName` varchar(100) DEFAULT NULL,
			`VersionModelNumber` varchar(100) DEFAULT NULL,
			`CatalogNumber` varchar(100) DEFAULT NULL,
			`DunsNumber` varchar(100) DEFAULT NULL,
			`CompanyName` varchar(500) DEFAULT NULL,
			`DeviceCount` varchar(200) DEFAULT NULL,
			`DeviceDescription` varchar(3000) DEFAULT NULL,
			`DMExempt` varchar(1100) DEFAULT NULL,
			`PremarketExempt` varchar(1500) DEFAULT NULL,
			`DeviceHCTP` varchar(1000) DEFAULT NULL,
			`DeviceKit` varchar(200) DEFAULT NULL,
			`DeviceCombinationProduct` varchar(250) DEFAULT NULL,
			`SingleUse` varchar(500) DEFAULT NULL,
			`LotBatch` varchar(50) DEFAULT NULL,
			`SerialNumber` varchar(250) DEFAULT NULL,
			`ManufacturingDate` varchar(20) DEFAULT NULL,
			`ExpirationDate` varchar(20) DEFAULT NULL,
			`DonationIdNumber` varchar(50) DEFAULT NULL,
			`LabeledContainsNRL` varchar(50) DEFAULT NULL,
			`LabeledNoNRL` varchar(50) DEFAULT NULL,
			`MRISafetyStatus` varchar(1000) DEFAULT NULL,
			`RX` varchar(50) DEFAULT NULL,
			`OTC` varchar(50) DEFAULT NULL,
			`DeviceSterile` varchar(50) DEFAULT NULL,
			`SterilizationPriorToUse` varchar(100) DEFAULT NULL,
			PRIMARY KEY (`PrimaryDI`),
			KEY `idx_stage_fda_device_primaryDI` (`PrimaryDI`(30))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        
        #Select Count(*),PrimaryDI from stage_fda_device group by PrimaryDI having count(*) > 1;

		### STAGE_FDA_IDENTIFIERS #############################################
		
			DROP TABLE IF EXISTS stage_fda_identifiers;
			
			CREATE TABLE `stage_fda_identifiers` (
			`PrimaryDI` varchar(23) DEFAULT NULL,
			`DeviceID` varchar(23) DEFAULT NULL,
			`DeviceIdType` varchar(50) DEFAULT NULL,
			`DeviceIdIssuingAgency` varchar(200) DEFAULT NULL,
			`ContainsDINumber` varchar(50) DEFAULT NULL,
			`PkgQuantity` varchar(50) DEFAULT NULL,
			`PkgDiscontinueDate` varchar(50) DEFAULT NULL,
			`PkgStatus` varchar(50) DEFAULT NULL,
			`PkgType` varchar(50) DEFAULT NULL,
			KEY `idx_stage_fda_Contacts_PrimaryDI` (`PrimaryDI`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			
            CREATE INDEX idx_stage_fda_Contacts_PrimaryDI ON stage_fda_identifiers (PrimaryDI(200));
        
        
        ### STAGE_FDA_CONTACTS ################################################
       
			DROP TABLE IF EXISTS stage_fda_contacts;
		   
			CREATE TABLE `stage_fda_contacts` (
			`PrimaryDI` varchar(23) DEFAULT NULL,
			`Phone` varchar(20) DEFAULT NULL,
			`PhoneExtension` varchar(50) DEFAULT NULL,
			`Email` varchar(500) DEFAULT NULL,
			KEY `idx_stage_fda_Contacts_PrimaryDI` (`PrimaryDI`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE INDEX idx_stage_fda_Contacts_PrimaryDI ON stage_fda_contacts (PrimaryDI(200));
            
		### STAGE_MARKETINGCODES #############################################
       
			DROP TABLE IF EXISTS stage_marketingcodes;
		
			CREATE TABLE `stage_marketingcodes` (
			`SplAcceptableTerm` varchar(400) DEFAULT NULL,
			`Code` varchar(10) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		### STAGE_INDICATIONS ###############################################
		
			DROP TABLE IF EXISTS stage_indications;
			
			CREATE TABLE `stage_indications` (
			`Drug_Name` varchar(100) DEFAULT NULL,
			`Drug_ID` varchar(50) DEFAULT NULL,
			`Ind_Name` varchar(500) DEFAULT NULL,
			`Ind_ID` varchar(50) DEFAULT NULL,
			`NCT` varchar(50) DEFAULT NULL,
			`Status` varchar(50) DEFAULT NULL,
			`Phase` varchar(50) DEFAULT NULL,
			`DetailedStatus` varchar(500) DEFAULT NULL,
			KEY `idx_Stage_Ind_Drug_Name` (`Drug_Name`),
			KEY `idx_Stage_Drug_ID` (`Drug_ID`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			
			CREATE INDEX idx_Stage_Ind_Drug_Name ON stage_indications (Drug_Name(100));
            CREATE INDEX idx_Stage_Drug_ID ON stage_indications (Drug_ID);

		### STAGE_DRUGVOCABULARY ###############################################
				
			DROP TABLE IF EXISTS stage_drugVocabulary;

			CREATE TABLE `stage_drugVocabulary` (
			`Drug_ID` varchar(50) DEFAULT NULL,
			`Drug_Name` varchar(100) DEFAULT NULL,
			`Accession_Numbers` varchar(500) DEFAULT NULL,
			`CAS` varchar(100) DEFAULT NULL,
			`UNII` varchar(100) DEFAULT NULL,
			`Synonyms` varchar(500) DEFAULT NULL,
			`Standard_InChI_Key` varchar(500) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            
            ### STAGE_CMS_SERVICE ###############################################
            
            DROP TABLE IF EXISTS `stage_cms_service`;
            
            CREATE TABLE `stage_cms_service` (
			`cpt_code` varchar(10) DEFAULT NULL,
			`service` varchar(184) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            
            ### STAGE_FDA_GMDN ###############################################
            
            DROP TABLE IF EXISTS `stage_fda_gmdn`;
            
            CREATE TABLE `stage_fda_gmdn` (
			`PrimaryDI` varchar(30) DEFAULT NULL,
			`gmdnPTName` varchar(128) DEFAULT NULL,
			`gmdnPTDefinition` varchar(1024) DEFAULT NULL,
			KEY `idx_tmp_gmdn_PrimaryDI` (`PrimaryDI`),
			KEY `idx_tmp_gmdn_gmdnPTName` (`gmdnPTName`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            
            ### STAGE_FDA_GMDN ###############################################
            
            DROP TABLE IF EXISTS `stage_fda_productcodes`;
            
            CREATE TABLE `stage_fda_productcodes` (
			`PrimaryDI` varchar(30) DEFAULT NULL,
			`productCode` varchar(3) DEFAULT NULL,
			`productCodeName` varchar(128) DEFAULT NULL,
			KEY `idx_stage_fda_productcodes_PrimaryDI_productCode` (`PrimaryDI`,`productCode`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		
END