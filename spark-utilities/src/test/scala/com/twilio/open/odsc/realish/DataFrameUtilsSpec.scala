package com.twilio.open.odsc.realish


import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.scalatest.{FunSuite, Matchers}

case class PercentileRow(value: Double)

class DataFrameUtilsSpec extends FunSuite with Matchers with SharedSparkSql {

  override def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("odsc-spark-utils")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.sql.session.timeZone", "UTC")
  }

  final val notRandomRandom = {
    val generator = new Random
    generator.setSeed(100L)
    generator
  }

  private[realish] val jeff = Author("a101", "Jeff Vandermeer", 50)
  private[realish] val brandon = Author("a102", "Brandon Sanderson", 43)

  test("should expand Array values in DataFrame") {
    val spark = sparkSql
    import spark.implicits._

    val examples = Seq(
      UserPersonality("uuid1", "Scott", Array("old","tall","tired","stressed")),
      UserPersonality("uuid2", "Joe", Array("young","short","energetic","stress-free"))
    )

    // convert to Dataset
    val users = spark.createDataset(examples)

    // expand the array
    val expandedTags = DataFrameUtils.expandArray[String](users.toDF(), col("tags"))

    val allColumns = expandedTags.columns.toSet

    allColumns.size shouldEqual 11
    allColumns.contains("tags_old") shouldBe true
    allColumns.contains("tags_young") shouldBe true
    allColumns.contains("tags_energetic") shouldBe true
    allColumns.contains("tags_stress-free") shouldBe true
  }

  test("should expand Array values in DataFrame using target list") {
    val spark = sparkSql
    import spark.implicits._

    val examples = Seq(
      UserPersonality("uuid1", "Scott", Array("old","stressed")),
      UserPersonality("uuid2", "Joe", Array("young","stress-free"))
    )

    // convert to Dataset
    val users = spark.createDataset(examples)

    // expand the array
    val expandedTags = DataFrameUtils.expandArray[String](users.toDF(), col("tags"), Some(
      Seq("old", "stressed", "young", "stress-free")
    ))

    val allColumns = expandedTags.columns.toSet

    allColumns.size shouldEqual 7
    allColumns.contains("tags_old") shouldBe true
    allColumns.contains("tags_stressed") shouldBe true
    allColumns.contains("tags_young") shouldBe true
    allColumns.contains("tags_stress-free") shouldBe true
  }

  test("should flatten deeply nested struct") {
    val spark = sparkSql
    import spark.implicits._

    val books = spark.createDataset(
      Seq(
        LibraryBook("book1", "Annihilation", jeff),
        LibraryBook("book2", "Authority", jeff),
        LibraryBook("book3", "Acceptance", jeff)
      )
    )

    val flattenedAuthors = DataFrameUtils.flattenStruct(books.toDF, "author")

    flattenedAuthors.createOrReplaceTempView("library")

    val res = spark.sql("select count(*) as stock, author_uuid from library group by author_uuid")

    /* shows how to extract the underlying types to do unapply from Row */
    val schemaRef = res.schema.fields.map {
      case StructField(name: String, _: LongType, required: Boolean, _) =>
        s"$name:long:$required"
      case StructField(name: String, _: StringType, required: Boolean, _) =>
        s"$name:string:$required"
      case _ =>
        ""
    }
    schemaRef.mkString("->") shouldEqual "stock:long:false->author_uuid:string:true"

    res.collect().map { case Row(c:Long, authorId:String) =>
      s"$authorId:$c"
    }.head shouldEqual "a101:3"

  }

  test("should find all distinct values for all columns across all rows of a given DataFrame") {
    implicit val spark: SparkSession = sparkSql
    import spark.implicits._

    val books = spark.createDataset(
      Seq(
        LibraryBook("book1", "Annihilation", jeff),
        LibraryBook("book2", "Authority", jeff),
        LibraryBook("book3", "Acceptance", jeff),
        LibraryBook("book4", "The Way of Kings", brandon),
        LibraryBook("book5", "Words of Radiance", brandon),
        LibraryBook("book6", "OathBringer", brandon)
      )
    )
    val flattenedAuthors = DataFrameUtils.flattenStruct(books.toDF, "author")
    val distinctColumnValues = DataFrameUtils.distinctColumnValues(flattenedAuthors)

    distinctColumnValues.show() // prints to stdout the table view of the sql df
    distinctColumnValues.where(col("name").equalTo("author")).head.getAs[Int]("distinct") shouldBe 2
  }

  test("should final all missing values for all columns across all rows of a given DataFrame") {

    implicit val spark: SparkSession = sparkSql
    import spark.implicits._

    val missingNameAuthor = Author("a102", null, 35)

    val books = spark.createDataset(
      Seq(
        LibraryBook("book1", "Annihilation", jeff),
        LibraryBook("book2", "Authority", jeff),
        LibraryBook("book3", "Acceptance", jeff),
        LibraryBook("book4", "The Way of Kings", brandon),
        LibraryBook("book5", "Words of Radiance", brandon),
        LibraryBook("book6", "OathBringer", brandon),
        LibraryBook("book7", "The Throne of Glass", missingNameAuthor),
        LibraryBook("book8", "The Queen of Fire", missingNameAuthor)
      )
    )


    val missingColumnValues = DataFrameUtils.missingColumnValues(
      DataFrameUtils.flattenStruct(books.toDF, "author")
    )
    missingColumnValues.show()

    missingColumnValues.where(col("name").equalTo("author_name")).head.getAs[Int]("missing") shouldBe 2

  }

  test("should generate percentiles from any Numeric column") {
    implicit val spark: SparkSession = sparkSql
    import spark.implicits._

    var counts: Int = 0
    val randos = (1 to 10000).map { value =>
      counts += 1
      PercentileRow(notRandomRandom.nextDouble() * (value - (0.1 * counts) ) )
    }

    val percentileRows = sparkSql.createDataset(randos)
    val df = percentileRows.toDF()

    val quartiles = DataFrameUtils.percentiles(df, col("value"), Array(0.25, 0.5, .75), 0.25)
    quartiles.show

    /*
    +----------+------------------+
    |percentile|             value|
    +----------+------------------+
    |       p25|0.3013465006377103|
    |       p50|1578.8636609338855|
    |       p75|  8935.55351845612|
    +----------+------------------+
    */

    val medianValue = quartiles
      .where(col("percentile").equalTo("p50"))
      .select(col("value"))
      .head()

    val test = medianValue match {
      case Row(value: Double) =>
        value > 1578.0d
      case _ => false
    }

    test shouldBe true


  }

}
