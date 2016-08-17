package com.thoughtworks.sparkFlatter

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.dongxiguo.fastring.Fastring.Implicits._

/**
  * Created by yqjfeng on 5/19/16.
  */
object Flatter {

  private def nestedValues(flattenRow: Row, hierarchicalSchema: StructType, start: Int = 0): (Seq[Any], Int) = {
    val builder = Seq.newBuilder[Any]
    val end = hierarchicalSchema.foldLeft(start) {
      case (i, StructField(_, dataType: StructType, _, _)) =>
        val (nestedRow, next) = nestedValues(flattenRow, dataType, i)
        builder += Row(nestedRow: _*)
        next
      case (i, StructField(_, dataType, _, _)) =>
        builder += flattenRow(i)
        i + 1
    }
    (builder.result(), end)
  }

  def nestedRow(flattenRow: Row, hierarchicalSchema: StructType): Row = {
    val (values, _) = nestedValues(flattenRow, hierarchicalSchema, 0)
    Row(values: _*)
  }

  def nestedDataFrame(flattenDataFrame: DataFrame, hierarchicalSchema: StructType): DataFrame = {
    flattenDataFrame.sqlContext.createDataFrame(
      flattenDataFrame.map { hierarchicalRow => nestedRow(hierarchicalRow, hierarchicalSchema) },
      hierarchicalSchema
    )
  }

  def nested[A: Encoder](flattenDataFrame: DataFrame): Dataset[A] = {
    nestedDataFrame(flattenDataFrame, implicitly[Encoder[A]].schema).as[A]
  }

  def flattenSchema(hierarchicalSchema: StructType, prefix: Vector[String] = Vector.empty): StructType = {
    StructType(hierarchicalSchema.flatMap {
      case StructField(name, dataType: StructType, _, _) =>
        flattenSchema(dataType, prefix :+ name)
      case StructField(name, dataType, nullable, metadata) =>
        Seq(StructField(fast"${prefix.mkFastring("/")}/$name".toString, dataType, nullable, metadata))
    })
  }

  private def flattenValues(hierarchicalRow: Row, hierarchicalSchema: StructType): Seq[Any] = {
    (0 until hierarchicalSchema.length).flatMap { i =>
      hierarchicalSchema(i) match {
        case StructField(_, dataType: StructType, _, _) =>
          val value = if (hierarchicalRow == null) {
            null
          } else {
            hierarchicalRow.getStruct(i)
          }
          flattenValues(value, dataType)
        case f =>
          val value = if (hierarchicalRow == null) {
            null
          } else {
            hierarchicalRow.get(i)
          }
          Seq(value)
      }
    }
  }

  def flattenRow(hierarchicalRow: Row, hierarchicalSchema: StructType): Row = {
    Row(flattenValues(hierarchicalRow, hierarchicalSchema): _*)
  }

  def flattenDataFrame(hierarchicalDataFrame: DataFrame): DataFrame = {
    val schema = hierarchicalDataFrame.schema
    if (schema == null) {
      throw new IllegalArgumentException
    }
    hierarchicalDataFrame.sqlContext.createDataFrame(
      hierarchicalDataFrame.map { hierarchicalRow => flattenRow(hierarchicalRow, schema) },
      flattenSchema(schema)
    )
  }

  def flatten(dataset: Dataset[_]): DataFrame = {
    flattenDataFrame(dataset.toDF)
  }

}
