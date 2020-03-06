postgre start:
postgres -D /usr/local/var/postgres

postgre stop:
pg_ctl -D /usr/local/var/postgres stop -s -m fast

postgre create user:
/usr/local/opt/postgres/bin/createuser -s postgres


