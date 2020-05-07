package sql

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, Tumble}


case class EcommerceLog(ts: Long)

object sqlTest1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val MyKafkaUtil = new MyKafkaUtil(env);
    //时间特性改为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val myKafkaConsumer: FlinkKafkaConsumer[String] = MyKafkaUtil.getConsumer("ECOMMERCE")
    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val ecommerceLogDstream: DataStream[EcommerceLog] = dstream.map { jsonString => JSON.parseObject(jsonString, classOf[EcommerceLog]) }

    ecommerceLogDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[EcommerceLog](Time.seconds(0L)) {
      override def extractTimestamp(element: EcommerceLog): Long = {
        element.ts
      }
    }).setParallelism(1)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val ecommerceLogTable = tableEnv.fromDataStream(ecommerceLogDstream)
    val table: Table = ecommerceLogTable.select("mid,ch").filter("ch='appstore'")
   val as:DataStream[(String,String)]=table.toString()[(String,String)]
  }
}

class MyKafkaUtil(env: StreamExecutionEnvironment) {
  def getConsumer(str: String): FlinkKafkaConsumer[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    new FlinkKafkaConsumer[String](str, new SimpleStringSchema(), properties)
  }


}