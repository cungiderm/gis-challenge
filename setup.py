#!/usr/bin/env python

import apt
import os
import glob
import subprocess
import logging
import ConfigParser
import urllib
import urlparse
import zipfile
import requests


# move to argparse later
log_path = './setup.log'
log_level = logging.DEBUG
configuration_path = './settings.py'


# setup config
configuration = ConfigParser.ConfigParser()
configuration.read(configuration_path)


# setup logger
log_format = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
logger.setLevel(log_level)

file_handler = logging.FileHandler(log_path)
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)


def run(command, use_os_system=False):
    try:
        if use_os_system:
            result = os.system(command)
        else:
            result = subprocess.call(command)
        if result != 0:
            logger.error('Unable to run {0}'.format(command))
            logger.error('Result: {0}'.format(result))
            raise SystemExit('Error when running {0}'.format(command))
        else:
            logger.debug('Successfully run: {0}'.format(command))
    except Exception, e:
        logger.error('Unable to run {0}'.format(command))
        logger.error(e)


def install(package_name):
    cache = apt.Cache()
    if cache[package_name].is_installed:
        logger.info('Package {0} has been installed. Skipping...'.format(package_name))
    else:
        cache.mark_install()
        try:
            cache.commit()
            logger.debug('{0} has been installed succesfully.'.format(package_name))
        except Exception, e:
            logger.error('Unable to install {0}'.format(package_name))
            logger.error(e)


def install_packages():
    packages = ['postgresql', 'postgresql-9.3-postgis-2.1']
    for package in packages:
        install(package)


def create_database(database_name, drop_if_exists=True):
    if drop_if_exists:
        run(['sudo', '-u', 'postgres', 'dropdb', '--if-exists', database_name])
    run(['sudo', '-u', 'postgres', 'createdb', database_name])


def create_database_extension(database_name, extension):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'CREATE EXTENSION IF NOT EXISTS {0}'.format(extension)])


def create_index(database_name, schema_name, table_name, fields, index_name, index_type):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'CREATE INDEX {0} ON {1}.{2} USING {3} ({4})'.format(index_name, schema_name, table_name, index_type, fields)])


def create_table(database_name, schema_name, table_name, fields):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'CREATE TABLE IF NOT EXISTS {0}.{1} ({2})'.format(schema_name, table_name, fields)])


def load_csv(source_path, database_name, schema_name, table_name, fields, format='csv', header='True', delimiter=',', quote='"'):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'COPY {0}.{1}({2}) FROM \'{3}\' WITH (FORMAT {4}, HEADER {5}, DELIMITER \'{6}\', QUOTE \'{7}\')'.format(schema_name, table_name, fields, source_path, format, header, delimiter, quote)])


def export_to_csv(destination_path, database_name, schema_name, table_name, format='csv', header='True', delimiter=',', quote='"'):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'COPY (SELECT * FROM {0}.{1}) TO \'{2}\' WITH (FORMAT {3}, HEADER {4}, DELIMITER \'{5}\', QUOTE \'{6}\')'.format(schema_name, table_name, destination_path, format, header, delimiter, quote)])


def drop_column(database_name, schema_name, table_name, column_name):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'ALTER TABLE {0}.{1} DROP COLUMN IF EXISTS {2}'.format(schema_name, table_name, column_name)])


def add_column(database_name, schema_name, table_name, column_name, column_type):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'ALTER TABLE {0}.{1} ADD {2} {3}'.format(schema_name, table_name, column_name, column_type)])


def update_column(database_name, schema_name, table_name, column_name, query):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'UPDATE {0}.{1} SET {2} = {3}'.format(schema_name, table_name, column_name, query)])


def import_shapefile(source_path, database_name, schema_name, table_name):
    run('shp2pgsql -s 4283:4326 -d {0} {1}.{2} | sudo -u postgres psql -d {3}'.format(source_path, schema_name, table_name, database_name), True)


def export_shapefile(destination_path, database_name, schema_name, table_name):
    run('sudo -u postgres pgsql2shp -f {0} {1} {2}.{3}'.format(destination_path, database_name, schema_name, table_name), True)


def export_geojson(source_path, destination_path):
    run(['ogr2ogr', '-f', 'GeoJSON', destination_path, source_path])


def export_mbtiles(source_path, destination_path):
    run(['/usr/local/bin/tippecanoe', '-o', destination_path, source_path])


def dump_database(database_name, dump_path):
    run('sudo -u postgres pg_dump -Fc {0} > {1}'.format(database_name, dump_path), True)


def build_database(db_name, db_extension):
    create_database(db_name)
    create_database_extension(db_name, db_extension)


def download_data(source_path, staging_path):
    file_name = os.path.basename(urlparse.urlparse(source_path)[2])
    logger.info('Start downloading {0}...'.format(file_name))
    target_path = staging_path + file_name
    logger.debug('Downloading data from {0} to {1}'.format(source_path, target_path))
    urllib.urlretrieve(source_path, target_path)
    logger.info('Finish downloading {0}...'.format(file_name))


def unzip_file(source_path, target_path, force=True):
    logger.info('Start unzipping {0} to {1}...'.format(source_path, target_path))
    zipf = zipfile.ZipFile(source_path, 'r')
    zipf.extractall(target_path)
    zipf.close()
    logger.info('Finish unzipping...')


def import_victorian_suburb_data(source_path, database_name, schema_name, table_name):
    logger.info('Start importing suburb data...')
    for f in glob.glob(os.path.join(source_path, '*.shp')):
        import_shapefile(f, database_name, schema_name, table_name)
    logger.info('Finish importing suburb data...')


def load_json_data(database_name, schema_name, table_name, data):
    for d in data:
        keys = ''
        values = ''
        for k, v in d.iteritems():
            keys += '{0}, '.format(str(k))
            values += '\'{0}\', '.format(str(v))
        keys = keys[:-2]
        values = values[:-2]
        run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
             '-c', 'INSERT INTO {0}.{1}({2}) VALUES ({3})'.format(schema_name, table_name, keys, values)])


def unpack_fields(table_definitions):
    fields = ''
    field_specs = ''
    for td in table_definitions:
        if td['id'] != '_id':
            fields += '{0}, '.format(td['id'])
            field_specs += '{0} {1}, '.format(td['id'], td['type'])
    fields = fields[:-2]
    field_specs = field_specs[:-2]
    return fields, field_specs


def add_primary_key(database_name, schema_name, table_name, column_name, column_type, sequence_name):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'CREATE SEQUENCE {0}'.format(sequence_name)])
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'ALTER TABLE {0}.{1} ADD {2} {3} NOT NULL DEFAULT nextval(\'{4}\')'.format(schema_name, table_name, column_name, column_type, sequence_name)])
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-c', 'ALTER TABLE {0}.{1} ADD CONSTRAINT pk_{1} PRIMARY KEY ({2})'.format(schema_name, table_name, column_name)])


def import_victorian_school_data(dataspecs_path, source_path, database_name, schema_name, table_name):
    logger.info('Start importing school data...')
    r = requests.get(dataspecs_path)
    data = r.json()
    fields, field_specs = unpack_fields(data['result']['fields'])
    create_table(database_name, schema_name, table_name, field_specs)
    load_csv(source_path, database_name, schema_name, table_name, fields)
    add_primary_key(database_name, schema_name, table_name, 'id', 'int', 'school_id_seq')
    logger.info('Finish importing school data...')


def execute_sql_file(database_name, sql_file):
    run(['sudo', '-u', 'postgres', 'psql', '-d', database_name,
         '-f', sql_file])


def setup():
    install_packages()
    build_database(configuration.get('database', 'db_name'),
                   configuration.get('database', 'db_extension'))
    data_sources = {'victorian_suburb_data': configuration.get('data', 'victorian_suburb_data'),
                    'victorian_school_dataspecs': configuration.get('data', 'victorian_school_dataspecs'),
                    'victorian_school_data': configuration.get('data', 'victorian_school_data')}
    download_data(data_sources['victorian_suburb_data'], configuration.get('data', 'staging_path'))
    download_data(data_sources['victorian_school_data'], configuration.get('data', 'staging_path'))
    zip_file_path = os.path.join(configuration.get('data', 'staging_path'), os.path.basename(urlparse.urlparse(data_sources['victorian_suburb_data'])[2]))
    unzip_file_path = os.path.join(configuration.get('data', 'staging_path'), os.path.basename(zip_file_path).split('.')[0])
    unzip_file(zip_file_path, unzip_file_path)
    import_victorian_suburb_data(unzip_file_path, configuration.get('database', 'db_name'), 'public', 'locality')
    import_victorian_school_data(data_sources['victorian_school_dataspecs'],
                                 os.path.join(configuration.get('data', 'staging_path') + os.path.basename(urlparse.urlparse(data_sources['victorian_school_data'])[2])),
                                 configuration.get('database', 'db_name'),
                                 'public', 'school')
    drop_column(configuration.get('database', 'db_name'), 'public', 'school', 'location')
    add_column(configuration.get('database', 'db_name'), 'public', 'school', 'location', 'geometry(Point, 4326)')
    update_column(configuration.get('database', 'db_name'), 'public', 'school', 'location', 'ST_Transform(ST_SetSRID(ST_MakePoint(x, y), 4283), 4326)')
    create_index(configuration.get('database', 'db_name'), 'public', 'locality', 'geom', 'idx_locality_geom', 'GIST')
    create_index(configuration.get('database', 'db_name'), 'public', 'school', 'location', 'idx_school_location', 'GIST')
    execute_sql_file(configuration.get('database', 'db_name'), 'deliverables/schools_by_locality.sql')
    execute_sql_file(configuration.get('database', 'db_name'), 'sql/vw_locality.sql')
    execute_sql_file(configuration.get('database', 'db_name'), 'sql/vw_school.sql')
    run(['sudo', 'rm', '-rf', '/tmp/deliverables/'])
    run(['sudo', 'mkdir', '/tmp/deliverables/'])
    run(['sudo', 'chown', 'postgres:postgres', '/tmp/deliverables/'])
    run(['sudo', 'chmod', '777', '/tmp/deliverables/'])
    export_to_csv(os.path.join(configuration.get('data', 'staging_path'), '/tmp/deliverables/schools_by_locality.csv'), configuration.get('database', 'db_name'), 'public', 'schools_by_locality')
    dump_database(configuration.get('database', 'db_name'), '/tmp/deliverables/postgis_export.dmp')
    export_shapefile('/tmp/deliverables/locality.shp', configuration.get('database', 'db_name'), 'public', 'locality')
    export_shapefile('/tmp/deliverables/school.shp', configuration.get('database', 'db_name'), 'public', 'school')
    export_geojson('/tmp/deliverables/locality.shp', '/tmp/deliverables/locality.geojson')
    export_geojson('/tmp/deliverables/school.shp', '/tmp/deliverables/school.geojson')
    export_mbtiles('/tmp/deliverables/locality.geojson', '/tmp/deliverables/localities.mbtiles')
    export_mbtiles('/tmp/deliverables/school.geojson', '/tmp/deliverables/schools.mbtiles')
    run(['sudo', 'cp', '-rf', '/tmp/deliverables/*', configuration.get('data', 'deliverables_path')])


if __name__ == '__main__':
    setup()
