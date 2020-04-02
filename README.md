# migrate_apollo_db

This is a hacky script to transfer the content from an [Apollo](https://github.com/GMOD/apollo) instance into another instance.

This is probably *NOT* bug-free, use it at your own risk. And try it on a test instance first. And make backups. And cross fingers.

It was tested on Apollo v2.5.0, there is *ABSOLUTELY NO* guarantee that it will work with any other version.

## How it works

The transfer is done exclusively using SQL queries. This allows to transfer all the features history and all the metadata (which is not possible using the API at the moment).

The whole script uses an SQL transaction on both source and destination databases:

 - whatever happens, the transaction on the source database is rolled back at the end (or upon error).
 - there is a commit on the destination database only if everything went fine, and if the `--doit` option is provided

It is quite fast, <1 minute usually for a ~500mb genome.

Users are transfered too if not present in the destination database. Remember to check permissions after the transfer is done.

The (jbrowse) data directories are not transfered by this script, so you will probably need to adapt the values in the `organism` table once the transfer is done, and copy/move the data dir manually.

## Installation

Just make a virtualenv and install there requirements:

```
pip install -r requirements.txt
```

## Usage

Without options, it runs in dry-run mode (no change to any database will be done):

```
python migrate.py postgresql://postgres:password@source.database.example.org:5432/postgres postgresql://postgres:password@destination.database.example.org:5432/postgres
```

If by luck you see no errors, you can really launch the transfer like this:

```
python migrate.py --doit postgresql://postgres:password@source.database.example.org:5432/postgres postgresql://postgres:password@destination.database.example.org:5432/postgres
```
