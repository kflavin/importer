SP_PREP_DEVICEMASTER = """
CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`{procedure_name}`()
BEGIN     
		DROP TABLE IF EXISTS tmp_prod_device_master;
		
		CREATE TABLE tmp_prod_device_master (
			  ProductKey_ID mediumint(9) DEFAULT NULL,
			  Device_ID varchar(50) DEFAULT NULL,
			  Product_code varchar(10) DEFAULT NULL,
			  PrimaryDI varchar(50) DEFAULT NULL,
			  DeviceID_Type varchar(50) DEFAULT NULL,
			  DeviceIDIssuingAgency varchar(50) DEFAULT NULL,
			  DeviceDescription varchar(2000) DEFAULT NULL,
			  ContainsDINumber varchar(50) DEFAULT NULL,
			  CompanyName varchar(120) DEFAULT NULL,
			  Phone varchar(20) DEFAULT NULL,
			  PhoneExtension bigint(20) DEFAULT NULL,
			  Email varchar(100) DEFAULT NULL,
			  BrandName varchar(80) DEFAULT NULL,
			  DunsNumber varchar(15) DEFAULT NULL,
			  PkgQuantity bigint(20) DEFAULT NULL,
			  PkgDiscontinueDate datetime DEFAULT NULL,
			  PkgType varchar(20) DEFAULT NULL,
			  PkgStatus varchar(50) DEFAULT NULL,
			  RX varchar(50) DEFAULT NULL,
			  OTC varchar(50) DEFAULT NULL  
		);
		
		INSERT INTO tmp_prod_device_master(ProductKey_ID, Device_ID,  PrimaryDI,   DeviceID_Type,   DeviceIDIssuingAgency,
				DeviceDescription,   ContainsDINumber,   CompanyName,   Phone,   PhoneExtension,   Email,
				BrandName,   DunsNumber,   PkgQuantity,   PkgDiscontinueDate,   PkgType,   PkgStatus,   RX,   
				OTC,Product_Code)
		SELECT DISTINCT
				ProductKey_ID, Device_ID,  PrimaryDI,   DeviceID_Type,   DeviceIDIssuingAgency,
				DeviceDescription,   ContainsDINumber,   CompanyName,   Phone,   PhoneExtension,   Email,
				BrandName,   DunsNumber,   PkgQuantity,   PkgDiscontinueDate,   PkgType,   PkgStatus,   RX,   
				OTC,Product_Code
		FROM 
				device_master 
		WHERE 
				End_Eff_Date is NULL;
		
		DROP INDEX idx_device_master_PrimaryDI ON {database}.device_master;
		
		CREATE INDEX idx_device_master_PrimaryDI ON {database}.device_master (PrimaryDI);

		/*JOIN 
				tmp_product_keys t 
		ON 
				d.PrimaryDI= t.product_id AND d.Product_Code = t.Product_Code;
		*/          

#-Product Code------------------------------------------

		DROP TABLE IF EXISTS {database}.tmp_product_code;                    
				
		CREATE TABLE {database}.tmp_product_code (
			ProductKey_ID mediumint(9) NOT NULL ,
			Product_ID varchar(50),
			Product_Type varchar(50),
			Product_code varchar(10)
		) ;
		
		CREATE INDEX idx_tmp_product_code_product_ID ON {database}.tmp_product_code (product_ID);
		
		INSERT INTO tmp_product_code (ProductKey_ID,Product_ID,Product_Code)
		SELECT 
				DISTINCT ProductKey_ID,Product_ID,Product_Code
		FROM 
				{database}.product_keys
		WHERE 
				IFNULL(PRODUCT_ID,'') <> '' and Product_Type = 'MEDICAL DEVICES' ;
		
#-Stage Device Master------------------------------------------

		DROP TABLE IF EXISTS {database}.tmp_stage_device_master;
				
		CREATE TABLE tmp_stage_device_master (
			  Device_ID varchar(50) DEFAULT NULL,
			  PrimaryDI varchar(50) DEFAULT NULL,
			  DeviceID_Type varchar(50) DEFAULT NULL,
			  DeviceIDIssuingAgency varchar(50) DEFAULT NULL,
			  DeviceDescription varchar(2000) DEFAULT NULL,
			  ContainsDINumber varchar(50) DEFAULT NULL,
			  CompanyName varchar(120) DEFAULT NULL,
			  BrandName varchar(80) DEFAULT NULL,
			  DunsNumber varchar(15) DEFAULT NULL,
			  PkgQuantity bigint(20) DEFAULT NULL,
			  PkgDiscontinueDate datetime DEFAULT NULL,
			  PkgType varchar(20) DEFAULT NULL,
			  PkgStatus varchar(50) DEFAULT NULL
		);
#warnings
		#Insert Data using Staging Tables            
		INSERT INTO {database}.tmp_stage_device_master(
				Device_ID,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,
				#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType
			   # Eff_Date,End_Eff_Date,Created_By
		)
		SELECT DISTINCT
				DeviceID,d.PrimaryDI,DeviceIDType,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType
				#CURDATE(),DATE_ADD(CURDATE(),INTERVAL 20 YEAR),0  
		FROM {database}.stage_fda_device d 	
		JOIN {database}.stage_fda_identifiers i ON d.primaryDI = i.PrimaryDI;	

#-Stage Device Master Final------------------------------------------

		DROP TABLE IF EXISTS {database}.tmp_stage_device_master_final;

		CREATE TABLE tmp_stage_device_master_final (
			  ProductKey_ID mediumint(9) DEFAULT NULL,
			  Device_ID varchar(50) DEFAULT NULL,
			  Product_Code varchar(10) DEFAULT NULL,
			  PrimaryDI varchar(50) DEFAULT NULL,
			  DeviceID_Type varchar(50) DEFAULT NULL,
			  DeviceIDIssuingAgency varchar(50) DEFAULT NULL,
			  DeviceDescription varchar(2000) DEFAULT NULL,
			  ContainsDINumber varchar(50) DEFAULT NULL,
			  CompanyName varchar(120) DEFAULT NULL,
			  BrandName varchar(80) DEFAULT NULL,
			  DunsNumber varchar(15) DEFAULT NULL,
			  PkgQuantity bigint(20) DEFAULT NULL,
			  PkgDiscontinueDate datetime DEFAULT NULL,
			  PkgType varchar(20) DEFAULT NULL,
			  PkgStatus varchar(50) DEFAULT NULL
		);
		
		INSERT INTO {database}.tmp_stage_device_master_final(
				ProductKey_ID,Device_ID,Product_Code,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,
				#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType)
				#Eff_Date,End_Eff_Date,Created_By)
		SELECT p.ProductKey_ID,Device_ID,p.Product_Code,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,
				#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType
				#CURDATE(),DATE_ADD(CURDATE(),INTERVAL 20 YEAR),0  
		FROM tmp_stage_device_master d 
		JOIN {database}.tmp_product_code p ON d.primaryDI = p.product_ID;
		
#-Stage Device Master , records to Invalidate------------------------------------------

		DROP TABLE IF EXISTS {database}.tmp_stage_device_master_RecordstoInvalidate;

		CREATE TABLE {database}.tmp_stage_device_master_RecordstoInvalidate (
			  ProductKey_ID mediumint(9) DEFAULT NULL,
			  Device_ID varchar(50) DEFAULT NULL,
			  Product_Code varchar(10) DEFAULT NULL,
			  PrimaryDI varchar(50) DEFAULT NULL,
			  DeviceID_Type varchar(50) DEFAULT NULL,
			  DeviceIDIssuingAgency varchar(50) DEFAULT NULL,
			  DeviceDescription varchar(2000) DEFAULT NULL,
			  ContainsDINumber varchar(50) DEFAULT NULL,
			  CompanyName varchar(120) DEFAULT NULL,
			  BrandName varchar(80) DEFAULT NULL,
			  DunsNumber varchar(15) DEFAULT NULL,
			  PkgQuantity bigint(20) DEFAULT NULL,
			  PkgDiscontinueDate datetime DEFAULT NULL,
			  PkgType varchar(20) DEFAULT NULL,
			  PkgStatus varchar(50) DEFAULT NULL
		);
		
		CREATE INDEX idx_stage_device_master_1 ON {database}.tmp_stage_device_master_RecordstoInvalidate 
		(Device_ID,DeviceID_Type,Product_Code,PrimaryDI);

		CREATE INDEX idx_tmp_prod_device_master_1 ON {database}.tmp_prod_device_master 
		(Device_ID,DeviceID_Type,Product_Code,PrimaryDI);
		
		SET SESSION optimizer_switch='block_nested_loop=off';
		
		INSERT INTO {database}.tmp_stage_device_master_RecordstoInvalidate(
				ProductKey_ID,Device_ID,Product_Code,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,
				#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType)
				#Eff_Date,End_Eff_Date,Created_By)
		SELECT k.ProductKey_ID,s.Device_ID,k.Product_Code,s.PrimaryDI,s.DeviceID_Type,
				s.DeviceIDIssuingAgency,s.DeviceDescription,s.ContainsDINumber,s.CompanyName,							
				s.BrandName,s.DunsNumber,s.PkgQuantity,s.PkgDiscontinueDate,s.PkgType
		FROM 
				{database}.tmp_stage_device_master_final s JOIN 
				{database}.tmp_prod_device_master k 		 
		ON
				s.Device_ID 			= k.Device_ID 				AND
				s.DeviceID_Type			= k.DeviceID_Type 			AND
				s.Product_Code  		= k.Product_Code 			AND
				s.PrimaryDI 			= k.PrimaryDI 				AND
				(
					s.DeviceIDIssuingAgency	!= k.DeviceIDIssuingAgency  OR 
					s.DeviceDescription		!= k.DeviceDescription		OR 
					s.ContainsDINumber		!= k.ContainsDINumber		OR 
					s.CompanyName			!= k.CompanyName			OR 
					s.BrandName				!= k.BrandName				OR 
					s.DunsNumber			!= k.DunsNumber				OR 
					s.PkgQuantity			!= k.PkgQuantity			OR 
					s.PkgDiscontinueDate	!= k.PkgDiscontinueDate		OR 
					s.PkgType				!= k.PkgType				
					#t.PkgStatus			!= K.PkgStatus				OR 
					#t.RX					!= K.RX						OR 
					#t.OTC					!= K.OTC
				)		;
					
#-Delete all records from stage which already exists in prod-----------------------------

		DELETE t.* FROM 
		   {database}.tmp_stage_device_master_final t JOIN 
		   {database}.tmp_prod_device_master k 
		ON 
			t.Device_ID 	= k.Device_ID 		AND
			t.DeviceID_Type	= k.DeviceID_Type 	AND
			t.Product_Code  = k.Product_Code 	AND
			t.PrimaryDI 	= k.PrimaryDI;
			
		
#-Insert only new records in prod-----------------------------
		INSERT INTO {database}.device_master(
				Device_ID,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,							
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType,
				Eff_Date,End_Eff_Date,Created_By
		)
		SELECT Device_ID,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,
				#Phone,PhoneExtension,Email,
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType,
				CURDATE(),NULL,0    
		FROM {database}.tmp_stage_device_master_final ;


 #-Update existing records which exist in prod but has different values-----------------------------
		UPDATE {database}.device_master d
		JOIN {database}.tmp_stage_device_master_RecordstoInvalidate r ON
			d.Device_ID 	= s.Device_ID 		AND
			d.DeviceID_Type	= s.DeviceID_Type 	AND
			d.Product_Code  = s.Product_Code 	AND
			d.PrimaryDI 	= s.PrimaryDI
		SET
			d.End_Eff_Date = CURDATE();
 
 #-Insert records which has changed values in prod-----------------------------
		INSERT INTO {database}.device_master(
				Device_ID,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,							
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType,
				Product_Code,ProductKey_ID,
				Eff_Date,End_Eff_Date,Created_By)
		SELECT  Device_ID,PrimaryDI,DeviceID_Type,DeviceIDIssuingAgency,
				DeviceDescription,ContainsDINumber,CompanyName,							
				BrandName,DunsNumber,PkgQuantity,PkgDiscontinueDate,PkgType,
				Product_Code,ProductKey_ID,
				CURDATE(),NULL,0    
		FROM {database}.tmp_stage_device_master_RecordstoInvalidate ;
            
END
"""