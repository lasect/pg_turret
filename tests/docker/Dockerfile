# Single-stage image that uses pre-built artifacts from the host
FROM postgres:18-bookworm

# Copy the built extension from the 'dist' directory created on the host
# The structure inside ./dist follows standard Postgres 18 paths
COPY dist/usr/lib/postgresql/18/lib/pg_turret.so /usr/lib/postgresql/18/lib/
COPY dist/usr/share/postgresql/18/extension/pg_turret.control /usr/share/postgresql/18/extension/
COPY dist/usr/share/postgresql/18/extension/pg_turret--*.sql /usr/share/postgresql/18/extension/

# Enable the extension in shared_preload_libraries
RUN echo "shared_preload_libraries = 'pg_turret'" >> /usr/share/postgresql/postgresql.conf.sample
