# How to install postgresql on pis

- `sudo apt install --yes postgresql php8.3-pgsql python3-psycopg python3-psycopg-pool`
- `sudo -u postgres psql --file=postgresql.init.sql`
- add `export PGDATABASE=arcterx` to your .bashrc
