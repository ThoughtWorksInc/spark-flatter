package com.thoughtworks.flatter

import org.scalatest._
import com.thoughtworks.flatter.FlatterSpec.{SharedSparkData}
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import resource.{DefaultManagedResource, ManagedResource, Resource}

/**
  * Created by yqjfeng on 5/19/16.
  */
class FlatterSpec extends FlatSpec with Matchers {

  it should "become flatten" in {
    for (sc <- SharedSparkData.sparkContext; sqlContext = new SQLContext(sc)) {
      import sqlContext.implicits._
      val dataset = sqlContext.createDataset(
        Seq(
          Root(Section1(123, "foo"), 0.123, Section2(1000000000000000L)),
          Root(Section1(345, "bar"), 2.88, Section2(-1L))
        )
      )
      dataset.schema should not be(null)
      val treeDataframe = dataset.toDF
      treeDataframe.schema should not be(null)
      val flattenDataframe = Flatter.flattenDataFrame(treeDataframe)
      val allRows = flattenDataframe.collect()
      allRows should have length (2)
      flattenDataframe.schema.fields should have length (4)
      val head = allRows.head
      head(0) should be(123)
      head(1) should be("foo")
      head(2) should be(0.123)
      head(3) should be(1000000000000000L)
      val last = allRows.last
      last(0) should be(345)
      last(1) should be("bar")
      last(2) should be(2.88)
      last(3) should be(-1L)
    }

  }

  it should "become nested" in {
    for (sc <- SharedSparkData.sparkContext; sqlContext = new SQLContext(sc)) {
      import sqlContext.implicits._

      val nestedSchema = implicitly[Encoder[Root]].schema
      val flattenSchema = Flatter.flattenSchema(nestedSchema)

      val flattenDataframe = sqlContext.createDataFrame(
        sc.makeRDD(
          Seq(
            Row(123, "foo", 0.123, 1000000000000000L),
            Row(345, "bar", 2.88, -1L)
          )
        ),
        flattenSchema)

      val nestedDataframe = Flatter.nestedDataFrame(flattenDataframe, nestedSchema)
      nestedDataframe.schema.fields should have length (3)

      val dataset = nestedDataframe.as[Root]
      val allRows = dataset.collect()
      allRows should have length (2)

      val head = allRows.head
      head.section1.intField should be(123)
      head.section1.stringField should be("foo")
      head.doubleField should be(0.123)
      head.section2.longField should be(1000000000000000L)
      val last = allRows.last
      last.section1.intField should be(345)
      last.section1.stringField should be("bar")
      last.doubleField should be(2.88)
      last.section2.longField should be(-1L)
    }

  }

}

final case class Root(section1: Section1, doubleField: Double, section2: Section2)

final case class Section1(intField: Int, stringField: String)

final case class Section2(longField: Long)

object FlatterSpec {


  /**
    * @author 杨博 (Yang Bo) &lt;pop.atry@gmail.com&gt;
    */
  object SharedSparkData {

    private def conf = new SparkConf().setAppName("Spark App").setMaster("local[2]")

    /**
      * Creates a [[ManagedResource]] that shared by all users for any type with a [[Resource]] type class implementation.
      *
      * There is only one instance of the resource at the same time for all the users.
      * The instance will be closed once no user is still using it.
      */
    def shared[A: Resource : Manifest](opener: => A): ManagedResource[A] = {
      @volatile var sharedReference: Option[(Int, A)] = None
      val lock = new AnyRef

      def acquire = {
        lock.synchronized {
          val (referenceCount, sc) = sharedReference match {
            case None =>
              val r = opener
              implicitly[Resource[A]].open(r)
              (1, r)
            case Some((oldReferenceCount, sc)) =>
              (oldReferenceCount + 1, sc)
          }
          sharedReference = Some((referenceCount, sc))
          sc
        }
      }

      val resource = new Resource[A] {

        override def close(r: A): Unit = {
          lock.synchronized {
            sharedReference match {
              case Some((oldReferenceCount, sc)) =>
                if (r != sc) {
                  throw new IllegalArgumentException
                }
                if (oldReferenceCount == 1) {
                  implicitly[Resource[A]].close(sc)
                  sharedReference = None
                } else {
                  sharedReference = Some((oldReferenceCount - 1, sc))
                }
              case None =>
                throw new IllegalStateException
            }
          }
        }
      }

      new DefaultManagedResource[A](acquire)(resource, implicitly[Manifest[A]])
    }

    implicit def sparkResource = new Resource[SparkContext] {
      override def close(r: SparkContext): Unit = {
        r.stop()
      }
    }

    val sparkContext = shared(new SparkContext(conf))

  }

}