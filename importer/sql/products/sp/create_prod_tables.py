SP_CREATE_PRODTABLES = """
CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`{procedure_name}`()
BEGIN
    DROP TABLE IF EXISTS `{database}`.gmdn_lookup;
    DROP TABLE IF EXISTS `{database}`.drug_master;
    DROP TABLE IF EXISTS `{database}`.product_keys;
    DROP TABLE IF EXISTS `{database}`.device_master;
    
    
    CREATE TABLE `gmdn_lookup` (
      `id` int(11) NOT NULL,
      `PrimaryDI` varchar(30) NOT NULL,
      PRIMARY KEY (`id`,`PrimaryDI`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    
    
    CREATE TABLE `product_keys` (
      `ProductKey_ID` mediumint(9) NOT NULL AUTO_INCREMENT,
      `Product_ID` varchar(50) DEFAULT NULL,
      `Product_Type` varchar(50) DEFAULT NULL,
      `Product_Name` varchar(226) DEFAULT NULL,
      `ProprietaryName` varchar(400) DEFAULT NULL,
      `NonProprietaryName` varchar(512) DEFAULT NULL,
      `Eff_Date` datetime DEFAULT NULL,
      `End_Eff_Date` datetime DEFAULT NULL,
      `Created_By` mediumint(9) DEFAULT NULL,
      `Created_Date` datetime DEFAULT CURRENT_TIMESTAMP,
      `Modified_Date` datetime DEFAULT NULL,
      PRIMARY KEY (`ProductKey_ID`),
      KEY `idx_PK_ProprietaryName` (`ProprietaryName`),
      KEY `idx_product_keys_product_ID` (`Product_ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


    CREATE TABLE `drug_master` (
      `Master_Drug_ID` mediumint(9) NOT NULL AUTO_INCREMENT,
      `ProductKey_ID` mediumint(9) DEFAULT NULL,
      `Client_Product_ID` varchar(255) DEFAULT NULL,
      `LabelerName` varchar(121) DEFAULT NULL,
      `ProductNDC` varchar(10) DEFAULT NULL,
      `ProprietaryName` varchar(226) DEFAULT NULL,
      `NonProprietaryName` varchar(512) DEFAULT NULL,
      `ProductTypeName` varchar(27) DEFAULT NULL,
      `Source_Type` enum('FDA','USER','FDA_DISAPPROVED') DEFAULT NULL,
      `Company_ID` varchar(12) DEFAULT NULL,
      `Marketing_Cat_Name` varchar(40) DEFAULT NULL,
      `Marketing_Cat_Name_Desc` varchar(40) DEFAULT NULL,
      `TE_Code` varchar(50) DEFAULT NULL,
      `Interpretation` varchar(255) DEFAULT NULL,
      `TE_Type` varchar(50) DEFAULT NULL,
      `NDC_Exclude_Flag` varchar(1) DEFAULT NULL,
      `Ind_Drug_ID` varchar(8) DEFAULT NULL,
      `Ind_Drug_Name` varchar(50) DEFAULT NULL,
      `Ind_Name` varchar(255) DEFAULT NULL,
      `Ind_Status` varchar(10) DEFAULT NULL,
      `Ind_NCT` varchar(50) DEFAULT NULL,
      `Ind_Phase` varchar(50) DEFAULT NULL,
      `Ind_DetailedStatus` varchar(500) DEFAULT NULL,
      `Eff_Date` datetime DEFAULT NULL,
      `End_Eff_date` datetime DEFAULT NULL,
      `Created_By` mediumint(9) DEFAULT NULL,
      `Created_Date` datetime DEFAULT CURRENT_TIMESTAMP,
      `Modified_Date` datetime DEFAULT NULL,
      PRIMARY KEY (`Master_Drug_ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


    CREATE TABLE `device_master` (
      `Master_Device_ID` mediumint(9) NOT NULL AUTO_INCREMENT,
      `ProductKey_ID` mediumint(9) DEFAULT NULL,
      `Device_ID` varchar(50) DEFAULT NULL,
      `Product_Code` varchar(10) DEFAULT NULL,
      `PrimaryDI` varchar(50) DEFAULT NULL,
      `DeviceID_Type` varchar(50) DEFAULT NULL,
      `DeviceIDIssuingAgency` varchar(50) DEFAULT NULL,
      `DeviceDescription` varchar(2000) DEFAULT NULL,
      `ContainsDINumber` varchar(50) DEFAULT NULL,
      `CompanyName` varchar(120) DEFAULT NULL,
      `Phone` varchar(20) DEFAULT NULL,
      `PhoneExtension` bigint(20) DEFAULT NULL,
      `Email` varchar(100) DEFAULT NULL,
      `BrandName` varchar(80) DEFAULT NULL,
      `DunsNumber` varchar(15) DEFAULT NULL,
      `PkgQuantity` bigint(20) DEFAULT NULL,
      `PkgDiscontinueDate` datetime DEFAULT NULL,
      `PkgType` varchar(20) DEFAULT NULL,
      `PkgStatus` varchar(50) DEFAULT NULL,
      `RX` varchar(50) DEFAULT NULL,
      `OTC` varchar(50) DEFAULT NULL,
      `Eff_Date` datetime DEFAULT NULL,
      `End_Eff_Date` datetime DEFAULT NULL,
      `Created_By` mediumint(9) DEFAULT NULL,
      `Created_Date` datetime DEFAULT CURRENT_TIMESTAMP,
      `Modified_Date` datetime DEFAULT NULL,
      PRIMARY KEY (`Master_Device_ID`),
      KEY `idx_device_master_PrimaryDI` (`PrimaryDI`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
END
"""