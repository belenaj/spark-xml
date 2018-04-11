package com.databricks.spark.xml;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.types._

object sparkXmlBug {
    def main (Args: Array[String]): Unit = {
        val input = "C:\\tmp\\myFileSmall.xml"

        val ss = SparkSession
          .builder()
          .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
          .appName("sparkXmlBug")
          .master("local[*]")
          .getOrCreate()




   /*   val customSchema =  StructType(Array(
        StructField("wantedTag", DataTypes.StringType, true, Metadata.empty)
      ))

      val customSchema =  StructType(
        StructField("wantedTag", DataTypes.StringType, true, Metadata.empty) :: Nil)
*/

        val df =  ss.read
          .format("com.databricks.spark.xml")
          .option("rowTag", "wantedTag")
         // .option("rowTag", "wantedTag")
          .load(input)


        val wantedTag = df.select("*")
      //val wantedTag = df.select("wantedTag")

        wantedTag.show()


      val CommercialModelType1 = df.head()//.mkString
      val CommercialModelType = wantedTag.head()//.mkString

      val index               = CommercialModelType.fieldIndex("_corrupt_record") //falla si no encuentra el literal
      println("CommercialModelType"->CommercialModelType.getString(index))
      //df.unpersist(false)
      //   anotherWantedTag.show()

    }
}
