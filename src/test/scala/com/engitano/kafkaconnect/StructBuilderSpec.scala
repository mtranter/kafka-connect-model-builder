package com.engitano.kafkaconnect

import com.engitano.kafkaconnect.StructBuilderSpec.{Company, Person, Rating, School}
import org.apache.kafka.connect.data.Struct
import org.scalatest.{FlatSpec, Matchers}

import collection.JavaConverters._

object StructBuilderSpec {
  case class Person(id: Int, name: String)
  case class Company(name: String, owner: Option[Person])
  case class School(name: String, students: List[Person])
  case class Rating(category: String, scores: Map[String, Int])
}

class StructBuilderSpec extends FlatSpec with Matchers {

  "The StructBuilder" should "build a simple struct" in {
    val testObj = Person(1, "Engitano Mark")
    val struct = KafkaConnectValueFor(testObj).asInstanceOf[Struct]

    struct.getInt32("id") should be(testObj.id)
    struct.getString("name") should be(testObj.name)
  }

  it should "Buid a nested optional struct with Some value" in {
    val testObj = Company("Engitano", Some(Person(1, "Engitano Mark")))
    val struct = KafkaConnectValueFor(testObj).asInstanceOf[Struct]

    struct.getString("name") should be(testObj.name)
    struct.schema().field("owner").schema().isOptional should be(true)

    val subPerson = struct.getStruct("owner")
    subPerson.getInt32("id") should be(testObj.owner.get.id)
    subPerson.getString("name") should be(testObj.owner.get.name)
  }

  it should "Buid a nested optional struct with None value" in {
    val testObj = Company("Engitano", None)
    val struct = KafkaConnectValueFor(testObj).asInstanceOf[Struct]

    struct.getString("name") should be(testObj.name)
    struct.schema().field("owner").schema().isOptional should be(true)

    val subPerson = struct.getStruct("owner")
    subPerson should be(null)
  }

  it should "Buid a nested list" in {
    val testObj = School("Engitano College", List(Person(1, "Engitano Mark")))
    val struct = KafkaConnectValueFor(testObj).asInstanceOf[Struct]

    struct.getString("name") should be(testObj.name)
    val students = struct.getArray[Struct]("students").asScala
    students.length should be(1)
    val student = students.head

    student.getInt32("id") should be(testObj.students.head.id)
    student.getString("name") should be(testObj.students.head.name)
  }

  it should "Buid a nested map struct" in {
    val testObj = Rating("languages", Map("Scala" -> 100, "C#" -> 85, "Java" -> 75))
    val struct = KafkaConnectValueFor(testObj).asInstanceOf[Struct]

    struct.getString("category") should be(testObj.category)
    val scores = struct.getMap[String, Int]("scores").asScala
    scores should be(Map("Scala" -> 100, "C#" -> 85, "Java" -> 75))
  }
}
