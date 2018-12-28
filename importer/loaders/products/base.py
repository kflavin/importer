import logging, sys, os, re, time
from collections import OrderedDict
import xml.etree.ElementTree as ET

from importer.loaders.base import BaseLoader, convert_date

logger = logging.getLogger(__name__)

class DeltaBaseLoader(BaseLoader):

    def __init__(self, *args, **kwargs):
        self.DELTA_Q = kwargs.pop("DELTA_Q")
        self.RETRIEVE_LEFT_Q = kwargs.pop("RETRIEVE_LEFT_Q")
        self.RETRIEVE_RIGHT_Q = kwargs.pop("RETRIEVE_RIGHT_Q")
        self.DELETE_Q = kwargs.pop("DELETE_Q")
        self.ARCHIVE_Q = kwargs.pop("ARCHIVE_Q")
        self.INSERT_Q = kwargs.pop("INSERT_Q")
        self.join_columns = kwargs.pop("join_columns")
        self.compare_columns = kwargs.pop("compare_columns")
        self.extra_lcols = kwargs.pop("extra_lcols")
        self.extra_rcols = kwargs.pop("extra_rcols", [])    # if needed in the future
        self.insert_new_columns = kwargs.pop("insert_new_columns")
        self.xform_left = kwargs.pop("xform_left")
        self.xform_right = kwargs.pop("xform_right")
        self.left_table_name = kwargs.pop("left_table_name")
        self.right_table_name = kwargs.pop("right_table_name")
        self.right_table_name_archive = kwargs.pop("right_table_name_archive")

        super().__init__(*args, **kwargs)

    def _load_xml_files(self, indir):
        files = []
        for f in os.listdir(indir):
            if f.endswith(".xml"):
                files.append(f)

        pattern = re.compile(".*Part(\d+)")

        # dirs_clean = [ d for d in dirs if d.endswith(".xml") ]
        try:
            files.sort(key = lambda key: int(pattern.match(key).groups()[0]))
        except Exception as e:
            logger.error("Bad filename pattern.")
            raise

        # files = [files[0]] # toggle to load one file only

        logger.info(f"Loading {len(files)} files...")
        logger.debug(files)
        return files

    def _delta_q(self, query, cursor=None):
        """
        Return a list of found rows and not found rows.  Query MUST return
        found row in column 1.  Found=1, Not Found=0.  Return two lists,
        found items, and not found items.
        """
        if not cursor:
            cursor = self.cursor

        try:
            cursor.execute(query)
        except Exception as e:
            raise

        found = []
        not_found = []
        for f in cursor:
            if f[0] == 0:
                not_found.append(f[1:])
            elif f[0] == 1:
                found.append(f[1:])
            else:
                logger.warn(f"I don't recognize f[0]={f[0]}!  Skipping...")

        return found, not_found

    def _perform_xform(self, rows, xforms):
        """
        Perform transformations on rows.  xform is a dict of transformations, where each key
        is the index in the row that needs to be operated on.
        """
        if xforms:
            # rows are in a dict
            if type(rows[0]) == dict:
                # loop over all rows, and check dictionary for column xforms
                for row in rows:
                    for key,value in row.items():
                        if key in xforms:
                            row[key] = xforms[key](value)
            else:
                # rows are in a list.  assume keys in xforms are integers corresponding to index.
                for i,row in enumerate(rows):
                    row_l = list(row)
                    for j,xform in xforms.items():
                        row_l[j-1] = xform(row_l[j-1])
                    row = tuple(row_l)
                    rows[i] = row

    def _row_subset(self, compare_columns, rows):
        """
        Take a list of compare_columns, which are columns to include in the comparison.  The
        rows are a list of dicts, where keys are column names.
        """
        subset_rows = []
        for row in rows:
            new_row = {}
            for k,v in row.items():
                if k in compare_columns:
                    new_row[k] = v
            subset_rows.append(new_row)
        return subset_rows

    def _rows_needing_updates(self, compare_columns, extra_lcols, extra_rcols, left, right):
        """
        Find rows that need updates based on a subset of columns (compare_columns)
        Return a list of rows to insert into the database, and include any additional columns
        from the left table that may be needed (insert_columns)
        """
        subset_left = self._row_subset(compare_columns, left)
        subset_right = self._row_subset(compare_columns, right)
        need_updates = []

        for i,row in enumerate(subset_left):
            if row not in subset_right:
                # ID exists, but the row has changed.  We'll append the new row, but first
                # see if there are any additional columns from the left table we want to include.
                # This applies when we're not comparing tables with the same table schema.
                for col in extra_lcols:
                    if col not in row:
                        row[col] = left[i][col]

                # # if we need to include extra columns from the right table in the future...
                # for col in extra_rcols:
                #     if col not in row:
                #         row[col] = right[i][col]

                need_updates.append(row)
            else:
                pass
        
        return need_updates

    def _build_and_or_query(self, query, columns, rows, **kwargs):
        """
        rows can be either dictionaries or list/tuples.

        **kwargs is the keyword arguments to replace in the SQL template
        """
        where_clause = []

        for row in rows:
            and_clause = []
            and_clause_s = ""

            for i,column in enumerate(columns):
                if type(row) == dict:
                    and_clause.append(f"{column}='{row[column]}'")
                else:
                    and_clause.append(f"{column}='{row[i]}'")
            
            and_clause_s = " AND ".join(and_clause)
            where_clause.append(and_clause_s)

        kwargs['where_clause'] = " OR ".join(where_clause)

        return query.format(**kwargs)

    def _build_retrieve_query(self, query, columns, rows, table_name):
        """
        Return a valid query to retrieve records
        rows can be either dictionaries or list/tuples.
        """
        where_clause = []

        for row in rows:
            and_clause = []
            and_clause_s = ""

            for i,column in enumerate(columns):
                if type(row) == dict:
                    if row[column]:
                        and_clause.append(f"{column}='{row[column]}'")
                    else:
                        and_clause.append(f"{column} is NULL")
                else:
                    if row[i]:
                        and_clause.append(f"{column}='{row[i]}'")
                    else:
                        and_clause.append(f"{column} is NULL")
            
            and_clause_s = " AND ".join(and_clause)
            where_clause.append(f"({and_clause_s})")

        where_clause_s = " OR ".join(where_clause)

        return query.format(table_name=table_name, where_clause=where_clause_s)

    def process_found(self, found):
        # These records need to be updated
        total_update_count = 0
        for c, batch in enumerate(super()._batcher(found)):
            logger.info("Check batch for updates {}".format(c+1))

            # Get records from left table
            q = self._build_retrieve_query(self.RETRIEVE_LEFT_Q, self.join_columns, batch, self.left_table_name)
            logger.debug(f"RETRIEVE_LEFT_Q: {q}")
            left = super()._query(q, self.get_cursor(dictionary=True))
            self._perform_xform(left, self.xform_left)
            logger.debug(left)
            
            # Get records from right table
            q = self._build_retrieve_query(self.RETRIEVE_RIGHT_Q, self.join_columns, batch, self.right_table_name)
            logger.debug(f"RETRIEVE_RIGHT_Q: {q}")
            right = super()._query(q, self.get_cursor(dictionary=True))
            self._perform_xform(right, self.xform_right)
            logger.debug(right)

            need_updates = self._rows_needing_updates(self.compare_columns, self.extra_lcols, self.extra_rcols, left, right)

            if need_updates:
                logger.info(f"{len(need_updates)} rows need updates")
                total_update_count += len(need_updates)

                # Archive rows with changes
                q = self._build_and_or_query(self.ARCHIVE_Q, self.join_columns, 
                    need_updates, table_name=self.right_table_name, archive_table_name=self.right_table_name_archive)
                logger.debug(f"ARCHIVE_Q: {q}")

                result = super()._submit_single_q(q, commit=False)

                # Delete rows with changes
                q = self._build_retrieve_query(self.RETRIEVE_RIGHT_Q, self.join_columns, need_updates, self.right_table_name)
                logger.debug(f"ARCHIVE_Q: {q}")
                result = super()._submit_single_q(q, commit=False)

                # Insert the new row
                q = self.INSERT_Q.format(table_name=self.right_table_name)
                result = super()._submit_batch(q, need_updates)

        return total_update_count

    def process_not_found(self, not_found):
        # These are new records that should be inserted
        total_insert_count = 0
        for c, batch in enumerate(super()._batcher(not_found)):
            logger.info(f"Submitting INSERT batch {c+1}, length: {len(batch)}")
            total_insert_count += len(batch)
            
            # Retrieve the full record we need from left table
            q = self._build_and_or_query(self.RETRIEVE_LEFT_Q, self.join_columns, batch, table_name=self.left_table_name)

            left = super()._query(q, self.get_cursor(dictionary=True))
            print(left)

            self._perform_xform(left, self.xform_left)
            new_records = self._row_subset(self.insert_new_columns, left)
            
            if new_records:
                q = self.INSERT_Q.format(table_name=self.right_table_name)
                print(q)
                print(new_records)
                result = super()._submit_batch(q, new_records)

        return total_insert_count

    def delta_table(self, do_updates, do_inserts):
        """
        Run a delta between an old table and a new table.  Return two lists of ID's.
            First: New records
            Second: Existing records that require updates
        """
        logger.info(f"Performing delta update of {self.left_table_name} and {self.right_table_name}")

        q = self.DELTA_Q.format(left_table_name=self.left_table_name, right_table_name=self.right_table_name)
        logger.debug(q)

        found, not_found = self._delta_q(q)
        logger.info(f"Check {len(found)} records for updates, insert {len(not_found)} new records")
        
        if found and do_updates:
            update_count = self.process_found(found)
            logger.info(f"Updated {update_count} records")

        if not_found and do_inserts:
            insert_count = self.process_not_found(self, not_found)
            logger.info(f"Inserted {insert_count} records")


