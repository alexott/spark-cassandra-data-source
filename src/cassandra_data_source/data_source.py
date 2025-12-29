"""PyCassandra Data Source implementation."""

from pyspark.sql.datasource import DataSource

from .writer import CassandraBatchWriter, CassandraStreamWriter


class CassandraDataSource(DataSource):
    """PySpark Data Source for Apache Cassandra."""

    @classmethod
    def name(cls):
        """Return the data source format name."""
        return "pycassandra"

    def __init__(self, options):
        """Initialize data source with options."""
        self.options = options

    def writer(self, schema, save_mode):
        """Return a batch writer instance."""
        return CassandraBatchWriter(self.options, schema)

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance."""
        return CassandraStreamWriter(self.options, schema)
