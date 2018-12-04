package ro.esolutions.livyspark;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CassandraProperties {
    private String host;
    private String table;
    private String keyspace;
}
