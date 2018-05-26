package com.engitano.kafkaconnect

import java.util
import java.util.{Date, UUID}

import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import collection.JavaConverters._

trait ValueFor[T] {
  def getValue(t: T): Any
}

trait LowPriorityValueFor {

  def getISO8601StringForDate(date: Date): String = {
    import java.text.SimpleDateFormat
    import java.util.{Locale, TimeZone}
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormat.format(date)
  }


  def default[T] = new ValueFor[T] {
    override def getValue(t: T): Any = t
  }

  implicit def stringValue = default[String]
  implicit def byteValue = default[Byte]
  implicit def intValue = default[Int]
  implicit def longValue = default[Long]
  implicit def floatValue = default[Float]
  implicit def doubleValue = default[Double]
  implicit def boolValue = default[Boolean]
  implicit def dateValue = new ValueFor[Date] {
    override def getValue(t: Date): Any = getISO8601StringForDate(t)
  }
  implicit def uuid = new ValueFor[UUID] {
    override def getValue(t: UUID): Any = t.toString
  }

  implicit def optionValue[T](implicit vf: ValueFor[T]) = new ValueFor[Option[T]] {
    override def getValue(t: Option[T]): Any = t match {
      case Some(vt) => vf.getValue(vt)
      case None => null
    }
  }

  implicit def listValue[C[_] <: Seq[_], T](implicit vf: ValueFor[T]) = new ValueFor[C[T]] {
    override def getValue(t: C[T]): Any = t.foldLeft(new util.ArrayList[Any]()){ (l,v) =>
      l.add(vf.getValue(v.asInstanceOf[T]))
      l
    }
  }

  implicit def mapValue[A,B](implicit vfa: ValueFor[A], vfb: ValueFor[B]) = new ValueFor[Map[A,B]] {

    override def getValue(t: Map[A,B]): Any = t.foldLeft(new util.HashMap[Any,Any]()) { (m,v) =>
      m.put(vfa.getValue(v._1), vfb.getValue(v._2))
      m
    }
  }

  implicit def structValue[T](implicit structBuilder: StructAppender[T], sf: SchemaFor[T]) = new ValueFor[T] {
    override def getValue(t: T): Any =
      structBuilder.build(t, new Struct(sf()))
  }
}

object ValueFor extends LowPriorityValueFor

trait StructAppender[T] {
  def build(t: T, struct: Struct): Struct
}

object StructAppender {

  def apply[T](implicit cp: Lazy[StructAppender[T]]) = cp.value

  implicit def hnilDefault = new StructAppender[HNil] {
    override def build(t: HNil, s: Struct): Struct = s
  }

  implicit def hconsDefault[Key <: Symbol, H, T <: HList]
  (implicit key: Witness.Aux[Key],
   headDef: Lazy[ValueFor[H]],
   tailDef: Lazy[StructAppender[T]]) = new StructAppender[FieldType[Key,H] :: T] {
    override def build(t: FieldType[Key, H] :: T, s: Struct): Struct = {
      val struct = tailDef.value.build(t.tail, s)
      struct.put(key.value.name, headDef.value.getValue(t.head))
    }
  }

  implicit def classAuxDefault[T, R](implicit gen: LabelledGeneric.Aux[T, R], conv: StructAppender[R]): StructAppender[T] = new StructAppender[T] {
    override def build(t: T, s: Struct): Struct = conv.build(gen.to(t), s)
  }

  implicit class PimpedCaseClass[T,R](t: T)(implicit gen: LabelledGeneric.Aux[T, R], conv: StructAppender[R], sf: SchemaFor[T]) {
    def buildConnectRecord() = {
      val schema = sf()
      val struct = new Struct(schema)
      StructAppender[T].build(t, struct)
    }
  }
}

trait StructBuilder[T] {
  def build(t: T): Any
}

object StructBuilder extends LowPriorityValueFor with LowPrioritySchemaFor {

  def apply[T](t: T)(implicit sf: SchemaFor[T], sa: StructAppender[T]): Struct = {
    val schema = sf()
    val struct = new Struct(schema)
    sa.build(t, struct)
  }
}