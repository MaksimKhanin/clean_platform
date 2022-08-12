FROM postgres:13.1
ENV POSTGRES_USER ${POSTGRES_USER}
ENV POSTGRES_PASSWORD ${POSTGRES_PASSWORD}

COPY initdb.sql /docker-entrypoint-initdb.d/
COPY ./manual_sectors.csv ./manual_sectors.csv