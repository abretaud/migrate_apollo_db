#!/usr/bin/env python

import argparse
import logging
import sys

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import text


logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


class Migrator():

    def __init__(self):
        self.parse_args()

        self.connect_dbs()

        self.max_id_inserted = 0

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--doit', help="Use this to really do the transfer (dry-run by default)", action="store_true")
        parser.add_argument('src', type=str, help="The source database to get content from (e.g. postgresql://postgres:password@localhost:5432/postgres)")
        parser.add_argument('dest', type=str, help="The destination database to insert content into (e.g. postgresql://postgres:password@localhost:5432/postgres)")
        self.args = parser.parse_args()

        if self.args.src == self.args.dest:
            log.error("Src and dest databases should not be the same.")
            sys.exit(1)

    def connect_dbs(self):
        self.connect_src()
        self.connect_dest()

    def connect_src(self):
        self.src_engine = create_engine(self.args.src)
        self.src_database = MetaData(bind=self.src_engine)

        self.src_session = scoped_session(sessionmaker(autocommit=False,
                                                       autoflush=False,
                                                       bind=self.src_engine))
        self.src_con = self.src_engine.connect()

    def connect_dest(self):
        self.dest_engine = create_engine(self.args.dest)
        self.dest_database = MetaData(bind=self.dest_engine)

        self.dest_session = scoped_session(sessionmaker(autocommit=False,
                                                        autoflush=False,
                                                        bind=self.dest_engine))
        self.dest_con = self.dest_engine.connect()

    def migrate_table(self, table, id_column='id', ignore_columns=[], col_mapping={}, skip_existing_key=None):

        log.info('Migrating table %s' % table)

        id_map = {}
        inserted = 0
        skipped = 0

        # Get max_id to start incrementing from
        max_id = 0
        if id_column:
            res = self.dest_session.execute('SELECT MAX(%s) FROM %s' % (id_column, table)).fetchone()
            max_id = res[0]
            if max_id is None:
                max_id = 0

        # Get the column names from src
        rows = self.src_session.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'" % (table))
        cols = []
        for row in rows:
            cols.append(row[0])

        # Collect existing records
        existing = {}
        if id_column is None and skip_existing_key is None:
            skip_existing_key = cols

        if skip_existing_key:
            fields = ','.join(skip_existing_key)
            if id_column:
                fields = id_column + ',' + fields

            rows = self.dest_session.execute('SELECT %s FROM %s' % (fields, table))

            for row in rows:
                if id_column:
                    existing[tuple(row[1:])] = row[0]
                else:
                    existing[tuple(row)] = None

        # Get the table content
        rows = self.src_session.execute('SELECT * FROM %s' % (table))

        values = []
        for row in rows:
            max_id += 1
            col_values = {}
            row_id = 0
            for col in cols:
                if id_column and col == id_column:
                    col_values[col] = max_id
                elif col in ignore_columns:
                    col_values[col] = None
                elif col in col_mapping:
                    if row[row_id] is None:
                        col_values[col] = None
                    else:
                        col_values[col] = col_mapping[col][row[row_id]]
                else:
                    col_values[col] = row[row_id]
                row_id += 1

            new_check_key = []
            if skip_existing_key:
                for key_col in skip_existing_key:
                    new_check_key.append(col_values[key_col])

            if tuple(new_check_key) in existing:
                if id_column:
                    id_map[row[id_column]] = existing[tuple(new_check_key)]
                skipped += 1
            else:
                values.append(col_values)
                if id_column:
                    id_map[row[id_column]] = max_id

                    if max_id > self.max_id_inserted:
                        self.max_id_inserted = max_id
                inserted += 1

        colf_list = ", ".join(cols)
        col_list = ":" + ", :".join(cols)
        statement = text("INSERT INTO %s (%s) VALUES (%s)" % (table, colf_list, col_list))

        for line in values:
            self.dest_session.execute(statement, line)

        log.info('    Inserted %s and skipped %s on table %s' % (inserted, skipped, table))

        return id_map

    def update_columns(self, table, id_column='id', columns=[], col_mapping={}, allowed_missing=[]):

        log.info('Updating columns %s on table %s' % (columns, table))

        # Get the column names
        rows = self.src_session.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'" % (table))
        cols = []
        for row in rows:
            cols.append(row[0])

        # Get the table content
        rows = self.src_session.execute('SELECT * FROM %s' % (table))

        values = []
        for row in rows:
            col_values = {}
            row_id = 0
            for col in cols:
                if col == id_column or (col in columns and col != id_column):
                    if col in col_mapping:
                        if row[row_id] is None:
                            col_values[col] = None
                        elif row[row_id] not in col_mapping[col]:
                            log.error("Could not find a mapped if for column %s and value %s..." % (col, row[row_id]))
                            if col in allowed_missing:
                                log.error("    ...removing wrong value")
                                col_values[col] = None
                            else:
                                log.error("    ...so we will crash soon.")
                        else:
                            col_values[col] = col_mapping[col][row[row_id]]
                    else:
                        col_values[col] = row[row_id]
                row_id += 1

            values.append(col_values)

        field_list = ""
        for col in columns:
            if col == id_column:
                continue
            if len(field_list):
                field_list += ", "
            field_list += "%s = :%s" % (col, col)
        statement = text("UPDATE %s SET %s WHERE %s = :%s" % (table, field_list, id_column, id_column))

        for line in values:
            self.dest_session.execute(statement, line)

        log.info('    Updated %s on table %s' % (len(values), table))

    def update_hibernate_sequence(self):
        res = self.dest_session.execute('SELECT last_value FROM hibernate_sequence').fetchone()
        hib_max_id = res[0]

        if hib_max_id < self.max_id_inserted:
            log.info('Updating hibernate_sequence to %s' % (self.max_id_inserted))
            self.dest_session.execute('ALTER SEQUENCE hibernate_sequence RESTART WITH %s' % self.max_id_inserted)

    def migrate(self):

        if not self.args.doit:
            log.info('Running in DRY-RUN mode. No changes to any database.')

        try:
            user_map = self.migrate_table('grails_user', skip_existing_key=['username'])

            db_map = self.migrate_table('db', skip_existing_key=['name'])
            cv_map = self.migrate_table('cv', skip_existing_key=['name'])

            analysis_map = self.migrate_table('analysis')
            analysis_feature_map = self.migrate_table('analysis_feature', ignore_columns=['feature_id'], col_mapping={'analysis_id': analysis_map})

            allele_map = self.migrate_table('allele', ignore_columns=['variant_id'])
            self.migrate_table('allele_info', col_mapping={'allele_id': allele_map})

            dbxref_map = self.migrate_table('dbxref', col_mapping={'db_id': db_map}, skip_existing_key=['accession', 'db_id'])
            cvterm_map = self.migrate_table('cvterm', col_mapping={'cv_id': cv_map, 'dbxref_id': dbxref_map}, skip_existing_key=['cv_id', 'dbxref_id', 'name'])

            publication_map = self.migrate_table('publication', col_mapping={'type_id': cvterm_map})

            feature_map = self.migrate_table('feature', ignore_columns=['status_id'], col_mapping={'reference_allele_id': allele_map, 'dbxref_id': dbxref_map})
            feature_property_map = self.migrate_table('feature_property', col_mapping={'feature_id': feature_map, 'type_id': cvterm_map})

            self.update_columns('feature', columns=['status_id'], col_mapping={'id': feature_map, 'status_id': feature_property_map})
            self.update_columns('allele', columns=['variant_id'], col_mapping={'id': allele_map, 'variant_id': feature_map})
            self.update_columns('analysis_feature', columns=['feature_id'], col_mapping={'id': analysis_feature_map, 'feature_id': feature_map})

            self.migrate_table('feature_dbxref', id_column=None, col_mapping={'feature_featuredbxrefs_id': feature_map, 'dbxref_id': dbxref_map})
            self.migrate_table('feature_grails_user', id_column=None, col_mapping={'feature_owners_id': feature_map, 'user_id': user_map})
            feature_relationship_map = self.migrate_table('feature_relationship', col_mapping={'parent_feature_id': feature_map, 'child_feature_id': feature_map})

            phenotype_map = self.migrate_table('phenotype', col_mapping={'assay_id': cvterm_map, 'observable_id': cvterm_map, 'cvalue_id': cvterm_map, 'attribute_id': cvterm_map})
            self.migrate_table('feature_feature_phenotypes', id_column=None, col_mapping={'feature_id': feature_map, 'phenotype_id': phenotype_map})
            environment_map = self.migrate_table('environment')

            genotype_map = self.migrate_table('genotype')
            self.migrate_table('feature_genotype', col_mapping={'genotype_id': genotype_map, 'cvterm_id': cvterm_map, 'feature_id': feature_map, 'chromosome_feature_id': feature_map})

            self.migrate_table('phenotype_statement', col_mapping={'genotype_id': genotype_map, 'phenotype_id': phenotype_map, 'publication_id': publication_map, 'environment_id': environment_map, 'type_id': cvterm_map})
            self.migrate_table('phenotype_cvterm', id_column=None, col_mapping={'phenotype_phenotypecvterms_id': phenotype_map, 'cvterm_id': cvterm_map})

            self.migrate_table('variant_info', col_mapping={'variant_id': feature_map})

            synonym_map = self.migrate_table('synonym', col_mapping={'type_id': cvterm_map})
            self.migrate_table('feature_synonym', col_mapping={'publication_id': publication_map, 'feature_synonyms_id': feature_map, 'synonym_id': synonym_map, 'feature_id': feature_map})

            featurecvterm_map = self.migrate_table('featurecvterm', col_mapping={'cvterm_id': cvterm_map, 'feature_id': feature_map, 'publication_id': publication_map})

            go_annotation_map = self.migrate_table('go_annotation', col_mapping={'feature_id': feature_map})
            self.migrate_table('go_annotation_grails_user', id_column=None, col_mapping={'go_annotation_owners_id': go_annotation_map, 'user_id': user_map})

            organism_map = self.migrate_table('organism')
            sequence_map = self.migrate_table('sequence', col_mapping={'organism_id': organism_map})
            feature_location_map = self.migrate_table('feature_location', col_mapping={'sequence_id': sequence_map, 'feature_id': feature_map})

            self.migrate_table('feature_publication', id_column=None, col_mapping={'publication_id': publication_map, 'feature_id': feature_map})

            self.migrate_table('feature_location_publication', id_column=None, col_mapping={'feature_location_feature_location_publications_id': feature_location_map, 'publication_id': publication_map})

            self.migrate_table('sequence_chunk', col_mapping={'sequence_id': sequence_map})

            organismdbxref_map = self.migrate_table('organismdbxref', col_mapping={'organism_id': organism_map, 'dbxref_id': dbxref_map})

            self.migrate_table('preference', col_mapping={'organism_id': organism_map, 'sequence_id': sequence_map, 'user_id': user_map})

            self.migrate_table('featurecvterm_publication', id_column=None, col_mapping={'publication_id': publication_map, 'featurecvterm_id': featurecvterm_map})

            self.migrate_table('featurecvterm_dbxref', id_column=None, col_mapping={'dbxref_id': dbxref_map, 'featurecvterm_id': featurecvterm_map})

            self.migrate_table('feature_property_publication', id_column=None, col_mapping={'feature_property_feature_property_publications_id': feature_property_map, 'publication_id': publication_map})

            self.migrate_table('feature_relationship_feature_property', id_column=None, col_mapping={'feature_relationship_feature_relationship_properties_id': feature_relationship_map, 'feature_property_id': feature_property_map})

            user_group_map = self.migrate_table('user_group', skip_existing_key=['name'])
            self.migrate_table('user_group_users', id_column=None, col_mapping={'user_group_id': user_group_map, 'user_id': user_map})
            self.migrate_table('user_group_admin', id_column=None, col_mapping={'user_group_id': user_group_map, 'user_id': user_map})

            role_map = self.migrate_table('role', skip_existing_key=['name'])
            self.migrate_table('role_permissions', id_column=None, col_mapping={'role_id': role_map})
            self.migrate_table('grails_user_roles', id_column=None, col_mapping={'role_id': role_map, 'user_id': user_map})

            self.migrate_table('permission', col_mapping={'organism_id': organism_map, 'user_id': user_map, 'group_id': user_group_map})

            organism_property_map = self.migrate_table('organism_property')
            self.migrate_table('organism_organism_property', id_column=None, col_mapping={'organism_id': organism_map, 'organism_organism_property_id': organism_property_map})
            self.migrate_table('organism_property_organism_property', id_column=None, col_mapping={'organism_property_organism_properties_id': organism_property_map, 'organism_property_id': organism_property_map})
            self.migrate_table('organism_property_organismdbxref', id_column=None, col_mapping={'organismdbxref_id': organismdbxref_map, 'organism_property_organismdbxrefs_id': organism_property_map})

            feature_event_map = self.migrate_table('feature_event', col_mapping={'editor_id': user_map})
            self.update_columns('feature_event', columns=['child_id', 'child_split_id', 'parent_id', 'parent_merge_id'], col_mapping={'id': feature_event_map, 'child_id': feature_event_map, 'child_split_id': feature_event_map, 'parent_id': feature_event_map, 'parent_merge_id': feature_event_map}, allowed_missing=['child_id'])

            feature_type_map = self.migrate_table('feature_type', skip_existing_key=['name'])

            canned_comment_map = self.migrate_table('canned_comment', skip_existing_key=['comment'])
            self.migrate_table('canned_comment_feature_type', id_column=None, col_mapping={'canned_comment_feature_types_id': canned_comment_map, 'feature_type_id': feature_type_map})
            available_status_map = self.migrate_table('available_status', skip_existing_key=['value'])
            self.migrate_table('available_status_feature_type', id_column=None, col_mapping={'available_status_feature_types_id': available_status_map, 'feature_type_id': feature_type_map})
            suggested_name_map = self.migrate_table('suggested_name', skip_existing_key=['name'])
            self.migrate_table('suggested_name_feature_type', id_column=None, col_mapping={'suggested_name_feature_types_id': suggested_name_map, 'feature_type_id': feature_type_map})
            canned_value_map = self.migrate_table('canned_value', skip_existing_key=['label'])
            self.migrate_table('canned_value_feature_type', id_column=None, col_mapping={'canned_value_feature_types_id': canned_value_map, 'feature_type_id': feature_type_map})
            canned_key_map = self.migrate_table('canned_key', skip_existing_key=['label'])
            self.migrate_table('canned_key_feature_type', id_column=None, col_mapping={'canned_key_feature_types_id': canned_key_map, 'feature_type_id': feature_type_map})

            self.migrate_table('organism_filter', col_mapping={'organism_id': organism_map, 'canned_key_id': canned_key_map, 'canned_value_id': canned_value_map, 'canned_comment_id': canned_comment_map, 'suggested_name_id': suggested_name_map, 'available_status_id': available_status_map})

            self.migrate_table('analysis_property', col_mapping={'analysis_id': analysis_map, 'type_id': cvterm_map})

            self.migrate_table('operation')
            self.migrate_table('reference')
            self.migrate_table('custom_domain_mapping')
            self.migrate_table('go_term', skip_existing_key=['name'])
            self.migrate_table('part_of')
            self.migrate_table('cvterm_path', col_mapping={'type_id': cvterm_map, 'cv_id': cv_map, 'subjectcvterm_id': cvterm_map, 'objectcvterm_id': cvterm_map})
            self.migrate_table('cvterm_relationship', col_mapping={'type_id': cvterm_map, 'subjectcvterm_id': cvterm_map, 'objectcvterm_id': cvterm_map})
            self.migrate_table('dbxref_property', col_mapping={'type_id': cvterm_map, 'dbxref_id': dbxref_map})
            self.migrate_table('environmentcvterm', col_mapping={'cvterm_id': cvterm_map, 'environment_id': environment_map})
            self.migrate_table('phenotype_description', col_mapping={'type_id': cvterm_map, 'publication_id': publication_map, 'genotype_id': genotype_map, 'environment_id': environment_map})
            self.migrate_table('publication_author', col_mapping={'publication_id': publication_map})
            self.migrate_table('publication_relationship', col_mapping={'cvterm_id': cvterm_map, 'object_publication_id': publication_map, 'subject_publication_id': publication_map})
            self.migrate_table('publicationdbxref', col_mapping={'publication_id': publication_map, 'dbxref_id': dbxref_map})

            self.migrate_table('feature_relationship_publication', id_column=None, col_mapping={'publication_id': publication_map, 'feature_relationship_feature_relationship_publications_id': feature_relationship_map})

            # Tables intentionnaly not migrated:
            #   application_preference
            #   audit_log
            #   search_tool
            #   databasechangelog
            #   databasechangeloglock
            #   proxy
            #   track_cache
            #   sequence_cache
            #   server_data
            #   data_adapter
            #   data_adapter_data_adapter
            #   with_or_from

            self.update_hibernate_sequence()

        except:  # noqa: 722
            self.src_session.rollback()
            self.dest_session.rollback()
            raise

        if self.args.doit:
            log.info('Committing changes to dest database.')
            self.src_session.rollback()  # Make sure nothing is modified (even in case of bug)
            self.dest_session.commit()
        else:
            log.info('Running in DRY-RUN mode. No changes to any database.')
            self.src_session.rollback()  # Make sure nothing is modified (even in case of bug)
            self.dest_session.rollback()


if __name__ == '__main__':

    mig = Migrator()

    mig.migrate()
