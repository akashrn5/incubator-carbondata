package org.apache.carbondata.presto;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSink;
import io.prestosql.plugin.hive.HiveWriterFactory;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;

public class CarbondataPageSink implements ConnectorPageSink {

  @Override public long getCompletedBytes() {
    return 0;
  }

  @Override public long getSystemMemoryUsage() {
    return 0;
  }

  @Override public long getValidationCpuNanos() {
    return 0;
  }

  @Override public CompletableFuture<?> appendPage(Page page) {
    return null;
  }

  @Override public CompletableFuture<Collection<Slice>> finish() {
    return null;
  }

  @Override public void abort() {

  }
}
