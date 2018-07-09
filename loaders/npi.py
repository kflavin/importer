import csv
import itertools
import textwrap
from collections import OrderedDict
import mysql.connector as connector

class NpiLoader(object):
    """
    Load NPI data
    """

    def __init__(self, user, host, password, database, table_name="npi"):
        self.table_name = table_name

        self.cnx = connector.connect(user=user, password=password, host=host, database=database)
        self.cursor = self.cnx.cursor()

    def __clean_field(self, field):
        field_clean = ' '.join(field.split())   # replace multiple whitespace characters with one space
        field_clean = field_clean.replace("(", "")
        field_clean = field_clean.replace(")", "")
        field_clean = field_clean.replace(".", "")
        field_clean = field_clean.replace("", "")
        field_clean = field_clean.replace(" If outside US", "")
        field_clean = field_clean.replace(" ", "_")
        return field_clean

    def __clean_fields(self, fields):
        columns = []

        for field in fields:
            columns.append(self.__clean_field(field))

        return columns

    def create_table(self):
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS `{table_name}` (
            `NPI` int(11) NOT NULL,
            `Entity_Type_Code` int(1) DEFAULT NULL,
            `Replacement_NPI` int(11) DEFAULT NULL,
            `Employer_Identification_Number_EIN` varchar(9) DEFAULT NULL,
            `Provider_Organization_Name_Legal_Business_Name` varchar(70) DEFAULT NULL,
            `Provider_Last_Name_Legal_Name` varchar(35) DEFAULT NULL,
            `Provider_First_Name` varchar(20) DEFAULT NULL,
            `Provider_Middle_Name` varchar(20) DEFAULT NULL,
            `Provider_Name_Prefix_Text` varchar(5) DEFAULT NULL,
            `Provider_Name_Suffix_Text` varchar(5) DEFAULT NULL,
            `Provider_Credential_Text` varchar(20) DEFAULT NULL,
            `Provider_Other_Organization_Name` varchar(70) DEFAULT NULL,
            `Provider_Other_Organization_Name_Type_Code` varchar(1) DEFAULT NULL,
            `Provider_Other_Last_Name` varchar(35) DEFAULT NULL,
            `Provider_Other_First_Name` varchar(20) DEFAULT NULL,
            `Provider_Other_Middle_Name` varchar(20) DEFAULT NULL,
            `Provider_Other_Name_Prefix_Text` varchar(5) DEFAULT NULL,
            `Provider_Other_Name_Suffix_Text` varchar(5) DEFAULT NULL,
            `Provider_Other_Credential_Text` varchar(20) DEFAULT NULL,
            `Provider_Other_Last_Name_Type_Code` int(1) DEFAULT NULL,
            `Provider_First_Line_Business_Mailing_Address` varchar(55) DEFAULT NULL,
            `Provider_Second_Line_Business_Mailing_Address` varchar(55) DEFAULT NULL,
            `Provider_Business_Mailing_Address_City_Name` varchar(40) DEFAULT NULL,
            `Provider_Business_Mailing_Address_State_Name` varchar(40) DEFAULT NULL,
            `Provider_Business_Mailing_Address_Postal_Code` varchar(20) DEFAULT NULL,
            `Provider_Business_Mailing_Address_Country_Code` varchar(2) DEFAULT NULL,
            `Provider_Business_Mailing_Address_Telephone_Number` varchar(20) DEFAULT NULL,
            `Provider_Business_Mailing_Address_Fax_Number` varchar(20) DEFAULT NULL,
            `Provider_First_Line_Business_Practice_Location_Address` varchar(55) DEFAULT NULL,
            `Provider_Second_Line_Business_Practice_Location_Address` varchar(55) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_City_Name` varchar(40) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_State_Name` varchar(40) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_Postal_Code` varchar(20) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_Country_Code` varchar(2) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_Telephone_Number` varchar(20) DEFAULT NULL,
            `Provider_Business_Practice_Location_Address_Fax_Number` varchar(20) DEFAULT NULL,
            `Provider_Enumeration_Date` date DEFAULT NULL,
            `Last_Update_Date` date DEFAULT NULL,
            `NPI_Deactivation_Reason_Code` VARCHAR(2),
            `NPI_Deactivation_Date` date DEFAULT NULL,
            `NPI_Reactivation_Date` date DEFAULT NULL,
            `Provider_Gender_Code` varchar(1) DEFAULT NULL,
            `Authorized_Official_Last_Name` varchar(35) DEFAULT NULL,
            `Authorized_Official_First_Name` varchar(20) DEFAULT NULL,
            `Authorized_Official_Middle_Name` varchar(20) DEFAULT NULL,
            `Authorized_Official_Title_or_Position` varchar(35) DEFAULT NULL,
            `Authorized_Official_Telephone_Number` varchar(20) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_1` varchar(10) DEFAULT NULL,
            `Provider_License_Number_1` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_1` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_1` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_2` varchar(10) DEFAULT NULL,
            `Provider_License_Number_2` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_2` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_2` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_3` varchar(10) DEFAULT NULL,
            `Provider_License_Number_3` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_3` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_3` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_4` varchar(10) DEFAULT NULL,
            `Provider_License_Number_4` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_4` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_4` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_5` varchar(10) DEFAULT NULL,
            `Provider_License_Number_5` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_5` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_5` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_6` varchar(10) DEFAULT NULL,
            `Provider_License_Number_6` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_6` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_6` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_7` varchar(10) DEFAULT NULL,
            `Provider_License_Number_7` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_7` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_7` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_8` varchar(10) DEFAULT NULL,
            `Provider_License_Number_8` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_8` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_8` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_9` varchar(10) DEFAULT NULL,
            `Provider_License_Number_9` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_9` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_9` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_10` VARCHAR (10) DEFAULT NULL,
            `Provider_License_Number_10` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_10` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_10` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_11` varchar(10) DEFAULT NULL,
            `Provider_License_Number_11` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_11` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_11` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_12` varchar(10) DEFAULT NULL,
            `Provider_License_Number_12` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_12` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_12` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_13` varchar(10) DEFAULT NULL,
            `Provider_License_Number_13` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_13` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_13` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_14` varchar(10) DEFAULT NULL,
            `Provider_License_Number_14` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_14` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_14` varchar(1) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Code_15` varchar(10) DEFAULT NULL,
            `Provider_License_Number_15` varchar(20) DEFAULT NULL,
            `Provider_License_Number_State_Code_15` varchar(2) DEFAULT NULL,
            `Healthcare_Provider_Primary_Taxonomy_Switch_15` varchar(1) DEFAULT NULL,
            `Other_Provider_Identifier_1` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_1` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_1` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_1` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_2` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_2` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_2` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_2` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_3` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_3` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_3` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_3` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_4` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_4` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_4` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_4` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_5` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_5` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_5` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_5` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_6` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_6` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_6` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_6` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_7` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_7` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_7` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_7` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_8` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_8` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_8` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_8` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_9` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_9` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_9` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_9` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_10` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_10` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_10` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_10` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_11` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_11` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_11` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_11` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_12` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_12` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_12` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_12` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_13` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_13` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_13` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_13` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_14` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_14` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_14` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_14` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_15` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_15` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_15` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_15` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_16` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_16` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_16` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_16` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_17` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_17` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_17` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_17` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_18` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_18` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_18` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_18` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_19` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_19` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_19` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_19` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_20` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_20` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_20` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_20` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_21` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_21` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_21` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_21` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_22` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_22` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_22` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_22` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_23` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_23` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_23` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_23` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_24` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_24` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_24` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_24` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_25` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_25` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_25` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_25` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_26` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_26` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_26` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_26` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_27` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_27` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_27` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_27` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_28` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_28` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_28` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_28` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_29` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_29` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_29` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_29` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_30` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_30` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_30` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_30` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_31` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_31` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_31` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_31` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_32` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_32` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_32` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_32` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_33` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_33` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_33` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_33` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_34` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_34` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_34` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_34` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_35` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_35` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_35` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_35` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_36` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_36` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_36` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_36` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_37` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_37` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_37` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_37` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_38` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_38` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_38` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_38` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_39` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_39` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_39` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_39` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_40` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_40` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_40` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_40` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_41` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_41` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_41` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_41` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_42` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_42` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_42` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_42` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_43` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_43` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_43` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_43` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_44` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_44` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_44` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_44` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_45` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_45` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_45` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_45` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_46` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_46` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_46` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_46` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_47` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_47` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_47` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_47` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_48` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_48` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_48` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_48` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_49` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_49` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_49` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_49` varchar(80) DEFAULT NULL,
            `Other_Provider_Identifier_50` varchar(20) DEFAULT NULL,
            `Other_Provider_Identifier_Type_Code_50` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_State_50` varchar(2) DEFAULT NULL,
            `Other_Provider_Identifier_Issuer_50` varchar(80) DEFAULT NULL,
            `Is_Sole_Proprietor` varchar(1) DEFAULT NULL,
            `Is_Organization_Subpart` varchar(1) DEFAULT NULL,
            `Parent_Organization_LBN` varchar(70) DEFAULT NULL,
            `Parent_Organization_TIN` varchar(9) DEFAULT NULL,
            `Authorized_Official_Name_Prefix_Text` VARCHAR(5) DEFAULT NULL,
            `Authorized_Official_Name_Suffix_Text` VARCHAR(5) DEFAULT NULL,
            `Authorized_Official_Credential_Text` varchar(20) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_1` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_2` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_3` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_4` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_5` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_6` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_7` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_8` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_9` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_10` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_11` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_12` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_13` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_14` varchar(70) DEFAULT NULL,
            `Healthcare_Provider_Taxonomy_Group_15` varchar(70) DEFAULT NULL,
            `Modified_Timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`NPI`),
            KEY `Postal_Code` (`Provider_Business_Practice_Location_Address_Postal_Code`),
            KEY `LastName_PostalCode` (`Provider_Last_Name_Legal_Name`,`Provider_Business_Practice_Location_Address_Postal_Code`),
            KEY `FirstName_LastName` (`Provider_First_Name`,`Provider_Last_Name_Legal_Name`),
            KEY `Address_State` (`Provider_Business_Practice_Location_Address_State_Name`)
            )
            """.format(table_name=self.table_name)

        print(len(create_table_sql))

        self.cursor.execute(create_table_sql)
        self.cnx.commit()


    def insert_query(self, columns):
        cols = ""
        values = ""
        on_dupe_values = ""

        for column in columns:
            cols += "`{}`, ".format(column)
            values += "%({})s, ".format(column)
            on_dupe_values += "{} = VALUES({}), ".format(column, column)

        cols = cols.rstrip().rstrip(",")
        values = values.rstrip().rstrip(",")
        on_dupe_values = on_dupe_values.rstrip().rstrip(",")


        query = f"""
            INSERT INTO {self.table_name}
            ({cols})
            VALUES ({values})
            ON DUPLICATE KEY UPDATE
            {on_dupe_values}
        """

        print(textwrap.dedent(query))

        # query  = "INSERT INTO {} (".format(self.table_name)
        # query += cols
        # query += ") VALUES ("
        # query += values
        # query += ") ON DUPLICATE KEY UPDATE "
        # query += on_dupe_values

        return query

    def __submit_batch(self, query, data):
        # print("Execute query")
        try:
            # cursor.execute(sql, (arg1, arg2))
            self.cursor.executemany(query, data)
            # print("Commit query")
            self.cnx.commit()
        except:
            # print(self.cursor._last_executed)
            print(self.cursor.statement)
            raise
        # self.cursor.executemany(q, all_data)
        # self.cnx.commit()

    def step_load(self, infile, start, end=None):
        """
        For use with AWS step functions.  If your CSV has headers, start=0 will be your header.  It's
        up to the user to skip this row when using this function.
        """
        print("NPI loader for step functions.  Start = {}, End = {}".format(start, end))

        # Get the file headers first
        with open(infile, 'r') as headerFile:
            columnNames = csv.DictReader(headerFile).fieldnames

        fileh = open(infile, 'r')
        lines = [line for line in itertools.islice(fileh, start, end)]
        reader = csv.DictReader(lines, fieldnames=columnNames)

        # Our INSERT query
        q = self.insert_query(self.__clean_fields(columnNames))

        batch = []
        for row in reader:
            columns, values = zip(*row.items())
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
            batch.append(data)

        self.__submit_batch(q, batch)



    def step_load2(self, infile, position, batch_size):
        """
        This attempts to use file byte locations to split the file.  It is not working, because tell()
        can't be used in conjunction with the CSV module, which call next().
        """
        print("NPI loader for step functions.  Batch size = {}".format(batch_size))

        # Get the file headers first
        with open(infile, 'r') as headerFile:
            columnNames = csv.DictReader(headerFile).fieldnames

        # Our INSERT query
        q = self.insert_query(self.__clean_fields(columnNames))

        # Now proceed with the data
        fileh = open(infile, 'r')
        fileh.seek(position)
        
        reader = csv.DictReader(fileh, fieldnames=columnNames)

        batch = []
        for i, row in enumerate(reader):
            if not i < batch_size:
                break

            columns, values = zip(*row.items())
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
            batch.append(data)

        self.__submit_batch(q, batch)

        end_pos = fileh.tell()
        print("End position is {}".format(end_pos))
        return end_pos


    def load(self, infile, batch_size=1000):
        print("NPI loader, batch size = {}".format(batch_size))
        # reader = csv.DictReader(open(infile, 'r'))
        reader = csv.DictReader(infile)
        # headers = [ key for key in next(reader).keys() ]
        # q = self.insert_query(headers)
        q = self.insert_query(self.__clean_fields(reader.fieldnames))

        # all_data = []
        row_count = 0
        batch = []
        batch_count = 1

        for row in reader:
            if row_count >= batch_size:
                print("Submitting batch {}".format(batch_count))
                self.__submit_batch(q, batch)
                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            columns, values = zip(*row.items())

            # data = {key: value for key, value in row.items() }
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())

            # all_data.append(data)
            batch.append(data)

        # Get any remaining rows
        if batch:
            print("Submitting batch {}".format(batch_count))
            self.__submit_batch(q, batch)

        print("All done")