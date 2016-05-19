package com.thoughtworks.flatter

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}

/**
  * Created by yqjfeng on 5/19/16.
  */
object Flatter {
  def unflatten(flattenDataframe: DataFrame, treeSchema: StructType): DataFrame = ???

  def flattenSchema(treeSchema: StructType): StructType = {
    StructType(treeSchema.flatMap {
      case StructField(_, dataType: StructType, _, _) => flattenSchema(dataType)
      case f => Seq(f)
    })
  }

  private def flattenValues(treeRow: Row, treeSchema: StructType): Seq[Any] = {
    (0 until treeRow.length).flatMap { i =>
      treeSchema(i) match {
        case StructField(_, dataType: StructType, _, _) => flattenValues(treeRow.getStruct(i), dataType)
        case f => Seq(treeRow.get(i))
      }
    }
  }

  def flatten(treeRow: Row, treeSchema: StructType): Row = {
    Row(flattenValues(treeRow, treeSchema): _*)
  }

  def flatten(treeDataframe: DataFrame): DataFrame = {
    val schema = treeDataframe.schema
    if (schema == null) {
      throw new IllegalArgumentException
    }
    treeDataframe.sqlContext.createDataFrame(
      treeDataframe.map { treeRow => flatten(treeRow, schema) },
      flattenSchema(schema)
    )
  }

}
