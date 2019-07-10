CREATE DEFINER=`{user}`@`%` PROCEDURE `sp_Prep_Device_ProdKeys`()
BEGIN
    #--------------------------------------------------------------------------------------------*/
    # MEDICAL DEVICES
    #--------------------------------------------------------------------------------------------*/

    DROP TABLE IF EXISTS tmp_product_keys_final;

    CREATE TABLE tmp_product_keys_final(
            ID mediumint(8) NOT NULL AUTO_INCREMENT,
            Product_ID varchar(50),
            Product_Type varchar(50),
            Product_Name varchar(226) DEFAULT NULL,
            ProprietaryName varchar(3000) DEFAULT NULL,
            NonProprietaryName varchar(3000) DEFAULT NULL,
            PRIMARY KEY (`ID`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

    INSERT INTO tmp_product_keys_final (Product_ID,Product_Name,ProprietaryName,NonProprietaryName)
    SELECT
        DISTINCT c.PrimaryDI as Product_ID,#ProductCodeName as Product_Name,
        c.gmdnPTName as Product_Name,
        UPPER(DeviceDescription) as ProprietaryName,UPPER(DeviceDescription) as NonProprietaryName
        #d.PrimaryDI as ProprietaryID,
    FROM stage_fda_device  d
    JOIN stage_fda_gmdn c ON d.primaryDI = c.PrimaryDI
    WHERE IFNULL(DeviceDescription,'') <> '';

    DROP TABLE IF EXISTS tmp_product_keys_final_toCompare;

    CREATE TABLE tmp_product_keys_final_toCompare(
            Product_ID varchar(50),
            Product_Type varchar(50),
            Product_Name varchar(226) DEFAULT NULL,
         #   Product_Code varchar(10) DEFAULT NULL,
            ProprietaryName varchar(3000) DEFAULT NULL,
            NonProprietaryName varchar(3000) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

    INSERT INTO tmp_product_keys_final_toCompare (Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName)
    SELECT Product_ID,Product_Type,Product_Name,ProprietaryName,NonProprietaryName
    FROM product_keys where Product_Type = 'DEVICES' and product_id in (select distinct Product_ID from tmp_product_keys_final);

    #-----------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS tmp_product_keys_final_1;

    CREATE TABLE tmp_product_keys_final_1(
            Product_ID varchar(50),
            Product_Type varchar(50),
            Product_Name varchar(226) DEFAULT NULL,
         #   Product_Code varchar(10) DEFAULT NULL,
            ProprietaryName varchar(3000) DEFAULT NULL,
            NonProprietaryName varchar(3000) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;


    CREATE INDEX idx_tmp_product_keys_final_1_Product_Name ON tmp_product_keys_final_1 (Product_ID,Product_Name);
    CREATE INDEX idx_tmp_product_keys_final_toCompare_Product_Name ON tmp_product_keys_final_toCompare (Product_ID,Product_Name);

    SELECT @TotalRecords    := count(*) FROM tmp_product_keys_final;
    SET @PartitionofRecords := floor(@TotalRecords/3);
    SET @StartingID      := @PartitionofRecords;

    #SELECT @TotalRecords,@PartitionofRecords,@StartingID  ;

    WHILE @StartingID <= @TotalRecords
    DO

        INSERT INTO tmp_product_keys_final_1 (Product_ID,Product_Name,ProprietaryName,NonProprietaryName)
        SELECT Product_ID,Product_Name,ProprietaryName,NonProprietaryName FROM tmp_product_keys_final WHERE
        id <=  @StartingID;

        #select min(id),max(id) from tmp_product_keys_final;
        #select count(*) from tmp_product_keys_final;

        DELETE t.* FROM
        tmp_product_keys_final_1  t JOIN
        tmp_product_keys_final_toCompare  k ON
            t.Product_ID   = k.Product_ID   and
            t.Product_Name  = k.Product_Name ;

        #select max(id) from tmp_product_keys_final;
        #select @StartingID

        DELETE FROM tmp_product_keys_final WHERE id <= @StartingID;

        SELECT @StartingID := @StartingID + @PartitionofRecords ;

    END WHILE;

    INSERT INTO tmp_product_keys_final_1 (Product_ID,Product_Name,ProprietaryName,NonProprietaryName)
    SELECT Product_ID,Product_Name,ProprietaryName,NonProprietaryName FROM tmp_product_keys_final WHERE
    id <=  @StartingID;

    DELETE t.* FROM
    tmp_product_keys_final_1  t JOIN
    tmp_product_keys_final_toCompare  k ON
        t.Product_ID   = k.Product_ID   and
        t.Product_Name  = k.Product_Name ;

    INSERT INTO  product_keys(Product_Name,Product_ID,Product_Type,ProprietaryName,NonProprietaryName ,Eff_Date,End_Eff_Date,Created_By)
    SELECT DISTINCT Product_Name,Product_ID,'DEVICES' as Product_Type,ProprietaryName,NonProprietaryName,curdate(),NULL ,0 from tmp_product_keys_final_1;

    #Clean up
    DROP TABLE IF EXISTS tmp_product_keys_final;
    DROP TABLE IF EXISTS tmp_product_keys_final_1;
    DROP TABLE IF EXISTS tmp_product_keys_final_toCompare;

END