# Single-stage image that uses pre-built artifacts from the host
FROM postgres:18-bookworm

COPY dist/usr/lib/postgresql/18/lib/pg_turret.so /usr/lib/postgresql/18/lib/
COPY dist/usr/share/postgresql/18/extension/pg_turret.control /usr/share/postgresql/18/extension/
COPY dist/usr/share/postgresql/18/extension/pg_turret--*.sql /usr/share/postgresql/18/extension/

RUN echo "shared_preload_libraries = 'pg_turret'" >> /usr/share/postgresql/postgresql.conf.sample
