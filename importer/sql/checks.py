DISABLE = """
    SET foreign_key_checks = 0;
    SET UNIQUE_CHECKS = 0;
    SET AUTOCOMMIT = 0;
"""

ENABLE = """
    SET foreign_key_checks = 1;
    SET UNIQUE_CHECKS = 1;
    SET AUTOCOMMIT = 1;
"""