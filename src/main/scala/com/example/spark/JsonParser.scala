package com.example.spark

/**
  * This is simple Spark Application that:
  * 1. extracts Json data from the HDFS directory (input parameter)
  * 2. transforms them by means of the default Spark's Json parser
  * 3. loads the result to Hive database.table
  *
  * Since there is a problem with inserting data to a table with
  * spark properties (schema), the application reads the JSON schema
  * from the target table and creates the stage table.
  */

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.{Logger, LoggerFactory}


object JsonParser {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val property_key = "JsonSchema"

  def main(args: Array[String]): Unit = {

    new JCommander(Config, args.toArray: _*)

    val schema_table = Config.schema_table
    val schema_db = Config.schema_db
    val stage_table = Config.stage_table
    val stage_db = Config.stage_db

    val hdfs_location = Config.input
    val partition_num = Config.partition

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", stage_db)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Read json-schema from Hive tblproperties
    // Assumes that schema for parsing json files
    // is stored in table's DDL under JsonSchema key
    val catalog = spark.sessionState.catalog

    // Read table's DDL
    val table_ddl = catalog.getTableMetadata(new TableIdentifier(schema_table, Some(schema_db)))
    val tblproperties = table_ddl.properties

    val json_schema = tblproperties(property_key)
    val tableSchema = DataType.fromJson(json_schema).asInstanceOf[StructType]

    // Do something with the data, e.g.:
    spark.sql(s"drop table if exists $stage_db.$stage_table purge")
    spark.read
      .schema(tableSchema)
      .json(hdfs_location)
      .repartition(partition_num)
      .write
      .saveAsTable(s"$stage_db.$stage_table")

    spark.stop()
  }
}


object Config {
  @Parameter(names = Array("--schema_db"), required = true, description = "Database name")
  var schema_db: String = _

  @Parameter(names = Array("--schema_table"), required = true, description = "Table name")
  var schema_table: String = _

  @Parameter(names = Array("--stage_db"), required = true, description = "Database name")
  var stage_db: String = _

  @Parameter(names = Array("--stage_table"), required = true, description = "Table name")
  var stage_table: String = _

  @Parameter(names = Array("--partition_num"), required = true, description = "RDD partition number")
  var partition: Int = _

  @Parameter(names = Array("--input"), required = true, description = "HDFS input directory")
  var input: String = _
}

