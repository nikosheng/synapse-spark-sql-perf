package sample

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpch.{TPCH, TPCHTables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkTPCHTable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkTPCHTable")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Set:
    // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
    // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
    // val rootDir = "hdfs:///tpcds" // root directory of location to create data in.
    // Note: If you are using ADLS Gen2, the format shoule be like "abfs://<container>@<storageaccount>.dfs.core.windows/net/<folder>"
    val rootDir = "abfs://tpch@msftnikoadlsgen2storage.dfs.core.windows.net/data/100G"

    val databaseName = "tpch" // name of database to create.
    val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCHTables(sqlContext,
      generatorParams = Seq("-v"),
      dbgenDir = "/tmp/tpch-kit/tools", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType


    tables.genData(
      location = rootDir,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = 20) // how many dbgen partitions to run - number of input tasks.

    // Create the specified database
    sqlContext.sql(s"create database $databaseName")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
//    tables.analyzeTables(databaseName, analyzeColumns = true)
  }
}
