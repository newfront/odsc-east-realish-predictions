package com.twilio.open.odsc.realish

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameUtils {

  /**
    * Provides a default set of FallBack values for DataType
    * https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/types/package-summary.html
    */
  private[odsc] final val FallbackValues: Map[DataType, Column] = Map(
    StringType -> lit(""),
    ShortType -> lit(0),
    LongType -> lit(0L),
    DoubleType -> lit(0d),
    FloatType -> lit(0f),
    IntegerType -> lit(0),
    BooleanType -> lit(false)
  )

  // create a new DataFrame from spark RDD
  private[odsc] final val PercentileSchema = StructType(
    StructField("percentile", StringType, nullable = false) ::
      StructField("value", DoubleType, nullable = false) :: Nil)

  /* Encapsulates the p1, p25, median, p75, p90, p95, p99 */
  private[odsc] final val DefaultPercentileProbabilities: Array[Double] =
    Array(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)

  /**
    * Given an input DataFrame
    * @param df The DataFrame
    * @param col The DataFrame Column
    * @return The DataFrame with additional expanded array fields
    */
  def expandArray[T](df: DataFrame, col: Column, possibleValues: Option[Seq[T]] = None)(implicit valueEncoder: Encoder[T]): DataFrame = {
    val colName = col.toString()

    // note: this is expensive on very large datasets

    val values = possibleValues match {
      case Some(seq) =>
        seq
      case None =>
        df
          .selectExpr(s"explode($colName) as $colName")
          .select(col).map({ case Row(s:T) => s })
          .collect()
          .toSeq
    }

    val expandedRows = values.foldLeft[DataFrame](df)( (d, v: T) => {
      val column = colName + "_" + v
      d.withColumn(column, when(array_contains(col, v), 1).otherwise(0))
    })
    expandedRows
  }

  /**
    * Given a nested StructType
    * @param df The input DataFrame
    * @param columnName The target Column to target
    * @param fillMissingWith Defaults to a core set of DataTypes, pass Map.empty to not fill missing
    * @return The flattened StructType
    */
  def flattenStruct(df: DataFrame, columnName: String, fillMissingWith: Map[DataType, Column] = FallbackValues): DataFrame = {
    if (!df.columns.contains(columnName)) {
      df
    } else {
      df.select(s"$columnName.*").schema.fields.foldLeft[DataFrame](df)((d, structfield) => {
        val name = structfield.name
        val dataType = structfield.dataType
        val colName = s"$columnName.$name"
        val flattenedName = s"${columnName.replace('.', '_')}_$name"
        if (fillMissingWith.contains(dataType)) {
          d.withColumn(flattenedName, when(isnull(col(colName)), fillMissingWith(dataType)).otherwise(col(colName)))
        } else if (dataType.typeName == "struct") {
          flattenStruct(d, colName)
        } else {
          d
        }
      })
    }
  }

  /**
    * Given a DataFrame.
    * @param df The DataFrame to introspect
    * @param spark The implicit SparkSession
    * @return The distinct number of values across all rows for all column
    */
  def distinctColumnValues(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.schema.map { r =>
      (r.name, df.where(col(r.name).isNotNull)
        .select(col(r.name))
        .distinct()
        .count())
    }.toDF("name", "distinct")
  }

  /**
    * Given a DataFrame.
    * @param df The DataFrame to introspect
    * @param spark The implicit SparkSession
    * @return The number of missing values across all rows for all columns
    */
  def missingColumnValues(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.schema.map {
      case StructField(name: String, _: StringType, _, _) =>
        // will do isNull orEmpty check here - and count as missing values
        (name, df.where(col(name).isNull.or(col(name).equalTo(""))).count())
      case StructField(name: String, _, _, _) =>
        (name, df.where(col(name).isNull).count)
    }.toDF("name", "missing")
  }

  /**
    * Generate the underlying percentile distribution of a DataFrame column
    * @param df The DataFrame
    * @param col The target Column
    * @param probabilities The Probabilities Array (p25,p50,p75) would be Array(0.25,0.5,0.75)
    * @param relativeError The Relative Error (changes the backing sketch / approximation) - lower = more memory / less error
    * @return The Percentiles
    */
  def percentiles(df: DataFrame, col: Column,
                  probabilities: Array[Double] = DefaultPercentileProbabilities,
                  relativeError: Double = 0.25)(implicit spark: SparkSession): DataFrame = {
    val result = df
      .select(col.cast("Double")).as("value")
      .stat.approxQuantile("value", probabilities, relativeError)

    // iterate over the probabilities to create a dataframe with the p(1),p(25) etc
    val zipped = probabilities
      .sorted.map(v => (v * 100).toInt)
      .zip(result)
      .map( tuple => {

        val probability = s"p${tuple._1}"
        Row(probability, tuple._2)
      })

    spark.createDataFrame(spark.sparkContext.parallelize(zipped), PercentileSchema)

  }

}
