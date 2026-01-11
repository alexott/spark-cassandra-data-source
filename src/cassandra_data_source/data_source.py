"""PyCassandra Data Source implementation."""

from pyspark.sql.datasource import DataSource

from .reader import CassandraBatchReader
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

    def schema(self):
        """
        Return the schema of the data source.

        Connects to Cassandra and derives schema from table metadata.
        """
        # Create a temporary reader to get schema
        reader = CassandraBatchReader(self.options, schema=None)
        return reader.schema

    def reader(self, schema):
        """Return a batch reader instance."""
        return CassandraBatchReader(self.options, schema)

    def writer(self, schema, overwrite):
        """Return a batch writer instance."""
        return CassandraBatchWriter(self.options, schema)

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance."""
        return CassandraStreamWriter(self.options, schema)
