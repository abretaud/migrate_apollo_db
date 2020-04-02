FROM python:3.7-slim

MAINTAINER Anthony Bretaudeau <anthony.bretaudeau@inrae.fr>

ADD . /opt/migrate_apollo_db/

WORKDIR /opt/migrate_apollo_db

RUN pip3 install -r requirements.txt

CMD ["/usr/local/bin/python", "/opt/migrate_apollo_db/migrate.py", "--help"]
