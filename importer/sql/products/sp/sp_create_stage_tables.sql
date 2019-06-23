create procedure sp_create_stage_tables()
BEGIN
    CREATE TABLE `stage_rxnconso` (
      `RXCUI` varchar(8) NOT NULL,
      `LAT` varchar(3) NOT NULL DEFAULT 'ENG',
      `TS` varchar(1) DEFAULT NULL,
      `LUI` varchar(8) DEFAULT NULL,
      `STT` varchar(3) DEFAULT NULL,
      `SUI` varchar(8) DEFAULT NULL,
      `ISPREF` varchar(1) DEFAULT NULL,
      `RXAUI` varchar(8) NOT NULL,
      `SAUI` varchar(50) DEFAULT NULL,
      `SCUI` varchar(50) DEFAULT NULL,
      `SDUI` varchar(50) DEFAULT NULL,
      `SAB` varchar(20) NOT NULL,
      `TTY` varchar(20) NOT NULL,
      `CODE` varchar(50) NOT NULL,
      `STR` varchar(3000) NOT NULL,
      `SRL` varchar(10) DEFAULT NULL,
      `SUPPRESS` varchar(1) DEFAULT NULL,
      `CVF` varchar(50) DEFAULT NULL,
      KEY `idx_RXNCONSO_RXCUI` (`RXCUI`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

    CREATE TABLE `stage_rxnrel` (
      `RXCUI1` varchar(8) DEFAULT NULL,
      `RXAUI1` varchar(8) DEFAULT NULL,
      `STYPE1` varchar(50) DEFAULT NULL,
      `REL` varchar(4) DEFAULT NULL,
      `RXCUI2` varchar(8) DEFAULT NULL,
      `RXAUI2` varchar(8) DEFAULT NULL,
      `STYPE2` varchar(50) DEFAULT NULL,
      `RELA` varchar(100) DEFAULT NULL,
      `RUI` varchar(10) DEFAULT NULL,
      `SRUI` varchar(50) DEFAULT NULL,
      `SAB` varchar(20) NOT NULL,
      `SL` varchar(1000) DEFAULT NULL,
      `DIR` varchar(1) DEFAULT NULL,
      `RG` varchar(10) DEFAULT NULL,
      `SUPPRESS` varchar(1) DEFAULT NULL,
      `CVF` varchar(50) DEFAULT NULL,
      KEY `idx_RXNREL_RXCUI1_RXCUI2` (`RXCUI1`,`RXCUI2`),
      KEY `idx_RXNREL_RXCUI1_RXCUI2_RELA` (`RXCUI1`,`RELA`,`RXCUI2`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
END;

