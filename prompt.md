
## Initial prompt

I need to build a Spark Python data source that will allow to write and read to/from
Apache Cassandra database as official Spark Cassandra connector isn't actively maintained
and have problems with compatibility with Databricks runtimes.  Brainstorm and prepare a
plan for implementing a python data source with name `pycassandra`. In the initial
implementation try to support the minimal number of options supported by official Spark
Cassandra connector:
https://raw.githubusercontent.com/apache/cassandra-spark-connector/refs/heads/trunk/doc/reference.md. Let's
split the development into a few phases:

- only write to Cassandra (both batch and streaming). When writing data, check that data
  contains at least primary key of Cassandra table. When writing, perform data mapping if
  necessary (like, string -> uuid).
- allow to delete rows when writing when there are specific options are provided: name of
  the column that is deletion flag (it shouldn't be written into the table), and value
  that is the deletion flag.
- doing batch reading from Cassandra. If schema isn't provided, it should be obtained via
  cassandra driver mapping unsupported types (like, uuid -> string, etc.).  If schema is
  specified, check that it's matching to the structure of Cassandra table and return only
  specific columns. Check
  ~/work/samples/cassandra-dse-playground/driver-1.x/src/main/java/com/datastax/alexott/demos/TokenRangesScan.java
  file on how the full scan of Cassandra database should be done. Allow to specify an
  optional option that will specify additional filter expression that will be added to the
  token range expression via `AND` expression.
- implementation of the streaming reads will be done later after investigation on how
  we'll integrate with Cassandra CDC.
