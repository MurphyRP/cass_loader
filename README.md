OVERVIEW
========
I needed a declarative way for people to load some given tables with dummy data in Cassandra

USAGE
=====
./load_cass <yaml_file> <records_to_load> <cassandra_host>
./load_cass keyspace_file.yaml 10000 localhost

EXAMPLE FILE
============
name: cass_loader_test
tables:
    - name: users
      columns:
        id: uuid
        first_name: first_name
        last_name: last_name
        date_added: timestamp
        login_count: positive_int
        percent_success: double
        total_owned: decimal

ROADMAP
=======
* support for adding related tables
* cross keyspace updates
* parameters for custom connection properties

LICENSE
=======
Apache 2.0