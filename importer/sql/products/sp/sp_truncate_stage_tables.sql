CREATE DEFINER=`{user}`@`%` PROCEDURE `{database}`.`sp_truncate_stage_tables`()
BEGIN
	truncate table stage_cms_service;
    truncate table stage_drugVocabulary;
    truncate table stage_fda_contacts;
    truncate table stage_fda_device;
    truncate table stage_fda_gmdn;
    truncate table stage_fda_identifiers;
    truncate table stage_indications;
    truncate table stage_marketingcodes;
    truncate table stage_ndc_product;
    truncate table stage_orangebook_product;
END