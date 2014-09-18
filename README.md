OVERVIEW
========
I needed a declarative way for people to load some given tables with dummy data in Cassandra

USAGE
=====
./load_cass <yaml_file> <records_to_load>
./load_cass keyspace_file.yaml 10000

ROADMAP
=======
* support for adding related tables
* cross keyspace updates

LICENSE
=======
Apache 2.0