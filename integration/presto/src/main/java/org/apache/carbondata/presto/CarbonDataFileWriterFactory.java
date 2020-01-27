package org.apache.carbondata.presto;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;

import static java.util.Objects.requireNonNull;

public class CarbonDataFileWriterFactory implements HiveFileWriterFactory {

  private final HdfsEnvironment hdfsEnvironment;
  private final TypeManager typeManager;
  private final NodeVersion nodeVersion;
  private final FileFormatDataSourceStats stats;

  @Inject
  public CarbonDataFileWriterFactory(HdfsEnvironment hdfsEnvironment, TypeManager typeManager,
      NodeVersion nodeVersion, FileFormatDataSourceStats stats) {
    this(typeManager, hdfsEnvironment, nodeVersion, stats);
  }

  public CarbonDataFileWriterFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment,
      NodeVersion nodeVersion,
      FileFormatDataSourceStats stats)
  {
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
    this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    this.stats = requireNonNull(stats, "stats is null");
  }

  @Override
  public Optional<HiveFileWriter> createFileWriter(Path path, List<String> inputColumnNames,
      StorageFormat storageFormat, Properties schema, JobConf configuration,
      ConnectorSession session) {
    try {
      return Optional
          .of(new CarbonDataFileWriter(path, inputColumnNames, schema, configuration, typeManager));
    } catch (SerDeException e) {
      throw new RuntimeException("Error while creating carbon file writer", e);
    }
  }

}
