import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger

object multiDestination {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Writing data to multiple destinations")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val mySchema = StructType(Array(
      StructField("Id", IntegerType),
      StructField("Name", StringType)
    ))

    val askDF = spark
      .readStream
      .format("csv")
      .option("header", "true")
      .schema(mySchema)
      //      .option("checkpointLocation","/home/amulya/tmp")
      .load("/home/amulya/Desktop/csv/")
    //        .localCheckpoint()
    //println(askDF.show())
    //  println(askDF.isStreaming)
    //    println("Ask1")
    //      spark.sparkContext
    askDF.writeStream.foreachBatch { (askDF: DataFrame, batchId: Long) =>
      //        println("Ask2")
      //       askDF.localCheckpoint()
      askDF.persist()
      //          println("Ask3")
      askDF.write.format("avro").save("/home/amulya/Desktop/md1.avro")
      askDF.write.json("/home/amulya/Desktop/md2.json")
      askDF.write.parquet("/home/amulya/Desktop/md3.parquet")
      askDF.write.csv("/home/amulya/Desktop/md4.csv")
      askDF.unpersist()
    }.start().awaitTermination()

  }
}
