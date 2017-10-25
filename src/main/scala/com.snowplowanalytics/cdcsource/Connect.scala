package com.snowplowanalytics.cdcsource

import io.debezium.embedded.EmbeddedEngine
import io.debezium.config.Configuration
import java.util.function.Consumer

import org.apache.kafka.common.protocol.types.Type

import scala.collection.convert.decorateAsScala._
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.jackson.compactJson


object Connect {

  val offsetStorage = "/Users/chuwy/offset.dat"
  val dbStorage = "/Users/chuwy/dbhistory.dat"

  val config = Configuration.create()
    .`with`("connector.class", "io.debezium.connector.mysql.MySqlConnector")
    .`with`("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    .`with`("offset.storage.file.filename", offsetStorage)
    .`with`("offset.flush.interval.ms", 60000)
    .`with`("name", "my-sql-connector")

    .`with`("database.hostname", "127.0.0.1")
    .`with`("database.port", 3306)
    .`with`("database.user", "mysqluser")
    .`with`("database.password", "mysqlpw")

    .`with`("server.id", 85744)
    .`with`("database.server.name", "products")
    .`with`("database.history", "io.debezium.relational.history.FileDatabaseHistory")
    .`with`("database.history.file.filename", dbStorage)
    .build()

  val engine: EmbeddedEngine = EmbeddedEngine.create()
    .using(config)
    .notifying(new Printer)
    .build()

  case class Payload(beforeSchema: JValue, afterSchema: JValue, vendor: String)

  def fieldToSchema(field: Field): JValue = {
    if (field.schema() == Schema.BOOLEAN_SCHEMA) JObject(List("type" -> JString("boolean")))
    else if (field.schema() == Schema.STRING_SCHEMA) JObject(List("type" -> JString("string")))
    else if (field.schema() == Schema.OPTIONAL_STRING_SCHEMA) JObject(List("type" -> JString("string")))

    else if (field.schema() == Schema.INT8_SCHEMA) JObject(List("type" -> JString("integer")))
    else if (field.schema() == Schema.INT16_SCHEMA) JObject(List("type" -> JString("integer")))
    else if (field.schema() == Schema.INT32_SCHEMA) JObject(List("type" -> JString("integer")))
    else if (field.schema() == Schema.FLOAT32_SCHEMA) JObject(List("type" -> JString("number")))
    else if (field.schema() == Schema.FLOAT64_SCHEMA) JObject(List("type" -> JString("number")))

    else if (field.schema() == Schema.OPTIONAL_FLOAT32_SCHEMA) JObject(List("type" -> JString("number")))
    else if (field.schema() == Schema.OPTIONAL_FLOAT64_SCHEMA) JObject(List("type" -> JString("number")))

    else if (field.schema().`type`() == Schema.Type.STRUCT) JObject(List("type" -> JString("object"), "properties" -> extractSchema(field.schema())))
    else if (field.schema().`type`() == Schema.Type.ARRAY) JObject(List("type" -> JString("array"), "items" -> extractSchema(field.schema())))
    else JObject(Nil)
  }

  def extractSchema(schema: Schema) = {
    if (schema.`type`().isPrimitive) {
      JObject(Nil)
    } else {
      val fields = schema.fields().asScala.toList.map(field => field.name() -> fieldToSchema(field))
      JObject(fields)
    }
  }

  class Printer extends Consumer[SourceRecord] {
    override def accept(record: SourceRecord): Unit = {
      val kafkaSchemaBefore = record.valueSchema().field("before").schema()
      val kafkaSchemaAfter = record.valueSchema().field("after").schema()
      val schema = JObject("before" -> extractSchema(kafkaSchemaBefore), "after" -> extractSchema(kafkaSchemaAfter))

      println(compactJson(schema))
    }
  }
}
