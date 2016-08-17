package com.thoughtworks.sparkFlatter


import com.thoughtworks.sparkFlatter.EnumType.{EnumValue1, EnumValue2}

import scala.reflect.runtime.universe.TypeTag
import org.scalatest._
import com.thoughtworks.sparkFlatter.FlatterSpec.SharedSparkData
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkConf, SparkContext}
import resource.{DefaultManagedResource, ManagedResource, Resource}


/**
  * Created by yqjfeng on 5/19/16.
  */
class FlatterSpec extends FlatSpec with Matchers {

  it should "Dataset of enum" in {
    for (sc <- SharedSparkData.sparkContext; sqlContext = new SQLContext(sc)) {
      import sqlContext.implicits._

      val dataset = sqlContext.createDataset(
        Seq(
          EnumRow(EnumValue1),
          EnumRow(EnumValue2)
        )
      )
      print(dataset.schema)
      dataset.schema should not be (null)

    }
  }

  it should "become flatten" in {
    for (sc <- SharedSparkData.sparkContext; sqlContext = new SQLContext(sc)) {
      import sqlContext.implicits._
      val dataset = sqlContext.createDataset(
        Seq(
          Root(Section1(123, "foo"), 0.123, Section2(1000000000000000L)),
          Root(Section1(345, "bar"), 2.88, Section2(-1L))
        )
      )
      dataset.schema should not be (null)
      val treeDataframe = dataset.toDF
      treeDataframe.schema should not be (null)
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

//
//class EnumTypeMetadata[A] extends UserDefinedType[EnumType] {
//  override def sqlType: DataType = StringType
//
//  override def serialize(obj: Any): Any = {
//    obj.toString
//  }
//
//  override def userClass = classOf[EnumType]
//
//  override def deserialize(datum: Any): EnumType = {
//    datum match {
//      case "EnumValue1" => EnumValue1
//      case "EnumValue2" => EnumValue2
//    }
//  }
//}
//
//object UserDefinedTypeProxyFactory {
//
//  def createProxy[EnumTrait: TypeTag]: Class[UserDefinedType[EnumType]] = {
//    import scala.reflect.runtime.universe._
//    val enumType = typeOf[EnumTrait]
//    val enumTypeTree = TypeTree(enumType)
//    val code =
//
//    import scala.tools.reflect.ToolBox
//    val toolBox = scala.reflect.runtime.currentMirror.mkToolBox()
//    toolBox.eval(code).asInstanceOf[Class[UserDefinedType[EnumType]]]
//  }
//
//}

class UserDefinedTrait[T: TypeTag : Manifest] extends UserDefinedType[T] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): Any = UTF8String.fromString(obj.toString)

  override val userClass: Class[T] = manifest[T].runtimeClass.asInstanceOf[Class[T]]

  private val moduleInstances = {
    import scala.reflect.runtime.universe._
    (for {
      subclass <- typeOf[T].typeSymbol.asClass.knownDirectSubclasses
      if subclass.isModuleClass
    } yield {
      val module = subclass.owner.typeSignature.member(subclass.name.toTermName).asModule
      subclass.name.toString -> runtimeMirror(this.getClass.getClassLoader).reflectModule(module).asInstanceOf[T]
    })(collection.breakOut(Map.canBuildFrom))
  }

  override def deserialize(datum: Any): T = {
    moduleInstances(datum.toString)
  }

}

class RowUdt extends UserDefinedType[EnumRow] {
  type T = EnumRow
  override def sqlType: DataType = {
    import org.apache.spark.sql.catalyst.ScalaReflection._
    import scala.reflect.runtime.universe._
    val params = getConstructorParameters(typeOf[EnumRow])

    StructType(
          params.map { case (fieldName, fieldType) =>
            val Schema(dataType, nullable) = schemaFor(fieldType)
            StructField(fieldName, dataType, nullable)
          })
          throw new Exception
  }

  override def serialize(obj: Any): Any = {
    UTF8String.fromString(obj.toString)
        throw new Exception
      }

  override val userClass: Class[T] = classOf[EnumRow]



  override def deserialize(datum: Any): T = {

    EnumRow(EnumValue1)
    throw new Exception
    }

}

@SQLUserDefinedType(udt = classOf[EnumTypeUdt])
sealed trait EnumType

object EnumType {

  final case object EnumValue1 extends EnumType

  final case object EnumValue2 extends EnumType

}

class EnumTypeUdt extends UserDefinedTrait[EnumType]


@SQLUserDefinedType(udt = classOf[RowUdt])
final case class EnumRow(enum: EnumType)

object FlatterSpec {


  /**
    * @author 杨博 (Yang Bo) &lt;pop.atry@gmail.com&gt;
    */
  object SharedSparkData {

    private def conf = new SparkConf().setAppName("Spark App").setMaster("local[2]")

    // FIXME: Remove the following `shared` method once https://github.com/jsuereth/scala-arm/pull/46 is accepted and published
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
