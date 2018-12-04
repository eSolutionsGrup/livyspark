package ro.esolutions.livyspark;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import shade.com.datastax.spark.connector.google.common.collect.ImmutableMap;

import java.util.Objects;

public class AvroToCassandraJob implements Job<Long> {

    private final String path;
    private final CassandraProperties cassandra;

    public AvroToCassandraJob(final String path, final CassandraProperties cassandra) {
        this.path = Objects.requireNonNull(path, "path should not be null");
        this.cassandra = Objects.requireNonNull(cassandra, "cassandraProperties should not be null");
    }

    @Override
    public Long call(JobContext ctx) throws Exception {
        ctx.sc().hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");
        ctx.sc().getConf().set("spark.cassandra.connection.host", cassandra.getHost());

        Dataset<Row> df = ctx.sqlctx().read()
                .format("com.databricks.spark.avro")
                .load(path)
                .cache();

        df.write().format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", cassandra.getTable(), "keyspace", cassandra.getKeyspace()))
                .mode(org.apache.spark.sql.SaveMode.Append)
                .save();

        return df.count();
    }
}
