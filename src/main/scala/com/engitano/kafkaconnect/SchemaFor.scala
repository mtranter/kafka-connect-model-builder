package com.engitano.kafkaconnect

import java.util.Date

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait SchemaFor[T] {
  def apply(): Schema
}

trait LowPrioritySchemaFor {
  private def schemaFor[T](s: Schema) = new SchemaFor[T] {
    override def apply(): Schema = s
  }

  implicit val byteSchema = schemaFor[Byte](SchemaBuilder.int8())
  implicit val shortSchema = schemaFor[Short](SchemaBuilder.int16())
  implicit val intSchema = schemaFor[Int](SchemaBuilder.int32())
  implicit val longSchema = schemaFor[Long](SchemaBuilder.int64())
  implicit val floatSchema = schemaFor[Float](SchemaBuilder.float32())
  implicit val doubleSchema = schemaFor[Double](SchemaBuilder.float64())
  implicit val dateSchema = schemaFor[Date](SchemaBuilder.string())
  implicit val stringSchema = schemaFor[String](SchemaBuilder.string())
  implicit val booleanSchema = schemaFor[Boolean](SchemaBuilder.bool())

  implicit def optionalSchema[T](implicit sf: SchemaFor[T]): SchemaFor[Option[T]] = {
    val orig = sf()
    if(orig.`type`().isPrimitive) {
      schemaFor(new SchemaBuilder(orig.`type`()).optional())
    } else {
      val noob = orig.fields().asScala.foldLeft(new SchemaBuilder(orig.`type`())) { (sb, f) => sb.field(f.name(), f.schema()) }
      noob.optional()
      schemaFor[Option[T]](noob)
    }
  }

  implicit def collectionSchema[C[_] <: Iterable[_], T](implicit sf: SchemaFor[T]): SchemaFor[C[T]] = new SchemaFor[C[T]] {
    override def apply(): Schema = SchemaBuilder.array(sf())
  }

  implicit def mapSchema[Map[_,_], A,B](implicit sfa: SchemaFor[A], sfb: SchemaFor[B]): SchemaFor[Map[A,B]] =
    new SchemaFor[Map[A,B]] {
      override def apply(): Schema = SchemaBuilder.map(sfa(), sfb())
    }
}

object ValueEqualitySchemaBuilder {
  def struct() = new ValueEqualitySchemaBuilder(Schema.Type.STRUCT)
}

class ValueEqualitySchemaBuilder private(t: Schema.Type) extends SchemaBuilder(t) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Schema => {
      this.fields().asScala.forall(myF => other.fields().asScala.exists(of => myF.equals(of)))
    }
    case _ => false
  }
}

object SchemaFor extends LowPrioritySchemaFor {

  def apply[T](implicit cp: Lazy[SchemaFor[T]]) = cp.value.apply()

  implicit def hnilDefault = new SchemaFor[HNil] {
    override def apply(): Schema = ValueEqualitySchemaBuilder.struct()
  }

  implicit def hconsToSchema[Key <: Symbol, Head, Tail <: HList](
                                                                implicit key: Witness.Aux[Key],
                                                                headSchemaFor: Lazy[SchemaFor[Head]],
                                                                tailSchemaFor: Lazy[SchemaFor[Tail]])
  : SchemaFor[FieldType[Key, Head] :: Tail] =
    new SchemaFor[FieldType[Key, Head] :: Tail] {
      override def apply(): Schema = {
        val schema = tailSchemaFor.value().asInstanceOf[SchemaBuilder]

        schema.field(key.value.name, headSchemaFor.value())
        schema
      }
    }


      implicit def classAuxDefault[T,R](implicit gen: LabelledGeneric.Aux[T,R], conv: SchemaFor[R], ct: ClassTag[T]):
      SchemaFor[T] = new SchemaFor[T] {
        override def apply(): Schema = conv()
      }
}

