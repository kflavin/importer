CREATE DEFINER=`myusername`@`%` PROCEDURE `sp_kyle_prep_device_keys`()
BEGIN
            DROP TABLE IF EXISTS tmp_gmdn_id_generator;
            DROP TABLE IF EXISTS tmp_gmdn_id;
            DROP TABLE IF EXISTS tmp_gmdn_id_toCompare;
            
            DROP TABLE IF EXISTS tmp_gmdn_lookup;
            DROP TABLE IF EXISTS tmp_gmdn_lookup_toCompare;
            DROP TABLE IF EXISTS tmp_gmdn_lookup_pdi_name;
            DROP TABLE IF EXISTS tmp_product_key_id_name;
        
            -- GMDN ID Tables                
            CREATE TABLE tmp_gmdn_id(
					gmdnPTName varchar(150) NOT NULL,
					PRIMARY KEY (`gmdnPTName`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
           CREATE TABLE tmp_gmdn_id_toCompare(
					gmdnPTName varchar(150) NOT NULL,
					PRIMARY KEY (`gmdnPTName`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            CREATE TABLE tmp_gmdn_id_generator(
					id int NOT NULL AUTO_INCREMENT,
					gmdnPTName varchar(150) NOT NULL,
					PRIMARY KEY (`id`, `gmdnPTName`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            
           -- GMDN Lookup tables
           CREATE TABLE tmp_gmdn_lookup_pdi_name(
                    PrimaryDI varchar(30) NOT NULL,
                    gmdnPTName varchar(150) NOT NULL,
                    PRIMARY KEY `idx_tmp_gmdn_lookup_pdi_name_PrimaryDI` (`PrimaryDI`, `gmdnPTName`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            CREATE TABLE tmp_gmdn_lookup(
					id int NOT NULL,
                    PrimaryDI varchar(30) NOT NULL,
                    PRIMARY KEY `idx_tmp_gmdn_lookup_final_PrimaryDI` (`id`, `PrimaryDI`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            CREATE TABLE tmp_gmdn_lookup_toCompare(
					id int NOT NULL,
                    PrimaryDI varchar(30) NOT NULL,
                    PRIMARY KEY `idx_tmp_gmdn_lookup_toCompare_PrimaryDI` (`id`, `PrimaryDI`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            
            CREATE TABLE tmp_product_key_id_name (
					Product_ID varchar(50) not null,
					Product_Name varchar(150) not null,
					PRIMARY KEY (`Product_ID`),
					UNIQUE KEY `idx_tmp_product_key_id_name_Product_Name` (`Product_Name`)
			) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
            
            
            -- --------------------------------------
            -- Insert new GMDN rows into product_key
            -- --------------------------------------

		    -- Create rows for product_keys
            INSERT INTO tmp_gmdn_id (gmdnPTName) SELECT gmdnPTName from stage_fda_gmdn group by gmdnPTName;
            INSERT INTO tmp_gmdn_id_toCompare (gmdnPTName) SELECT Product_Name from prod.product_keys where Product_Type="DEVICES";
            
            -- for generating new ids. get the last row and insert it to set autoincrement
            -- To generate id's for new rows, get the last row inserted, and insert it into the generator table to "bump" the autoincrement ID
            -- Rows with Product_Type='DEVICES' id will always be an INT, because we are creating the id. (not all Product_ID's are INTs in the product_key table, though)
            SELECT  @last_product_id := max(convert(Product_ID, UNSIGNED INT)) from prod.product_keys where Product_Type="DEVICES";
            INSERT INTO tmp_gmdn_id_generator (id, gmdnPTName) SELECT Product_ID, Product_Name from prod.product_keys where Product_Type="DEVICES" and Product_ID = @last_product_id;
            
            SET sql_safe_updates=0;
			DELETE l FROM 
			tmp_gmdn_id 	l JOIN 
			tmp_gmdn_id_toCompare	c ON 
                l.gmdnPTName = c.gmdnPTName;
                
            -- Insert new rows into the generator table to generate a new id via auto-increment.  Remove the row we used to "prime" the table.
			INSERT INTO tmp_gmdn_id_generator (gmdnPTName) SELECT gmdnPTName from tmp_gmdn_id;
            DELETE FROM tmp_gmdn_id_generator where id = @last_product_id;
            INSERT INTO prod.product_keys (Product_ID, Product_Type, Product_Name) SELECT id, "DEVICES", gmdnPTName from tmp_gmdn_id_generator;
                
            -- ----------------------------------
            -- GMDN Lookup table
            -- ----------------------------------

            -- Create GMDN lookup table.  We need to combine Product_ID with PrimaryDI for lookups.  These values come from different tables: product_key and
            -- stage_fda_gmdn, respectively, and join on Product_Name and gmdnPTName.
            INSERT INTO tmp_gmdn_lookup_toCompare SELECT id, PrimaryDI from prod.gmdn_lookup; -- 7s
            INSERT INTO tmp_product_key_id_name SELECT Product_ID, Product_Name from product_keys where Product_Type="DEVICES";
            INSERT INTO tmp_gmdn_lookup_pdi_name SELECT PrimaryDI, gmdnPTName from stage_fda_gmdn; -- 12s on my EC2
            
            -- 9s completion
            INSERT INTO tmp_gmdn_lookup (id, PrimaryDI)
				SELECT Product_ID, PrimaryDI from tmp_product_key_id_name k
				JOIN tmp_gmdn_lookup_pdi_name l ON  k.Product_Name = l.gmdnPTName;
                            
            -- 9s compeltion
            DELETE l FROM 
			tmp_gmdn_lookup 	l JOIN 
			tmp_gmdn_lookup_toCompare c ON 
                l.id = c.id 	AND 
				l.PrimaryDI = c.PrimaryDI;
                
			INSERT INTO prod.gmdn_lookup (`id`, `PrimaryDI`) SELECT `id`, `PrimaryDI` from tmp_gmdn_lookup;
 END           
			
            