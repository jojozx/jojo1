package com.jd

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment


case class  DbGoods(goodId:Integer,catId:Int,goodsSn:String,
                    newGoodsSn:String ,goodsName:String,BrandId:String,
                    goodsNumber:String,goodsWeight:String,goorsBrief:String,
                    goodsThumb:String,goodsImg:String,addTime :Long,
                    isdelete:Int, goodsFormat:String,barcode:String,
                    packageFormat:String,factoryCode:String,minpackage:String,
                    productCompany:String,measureunit:String,conversionValue:String
                    ,minMeasureUnit:String,oeNum:String,repaireNum:String ,productRegion:String
                   ,brandPartcode : String,sellerId:Int,sellerNick:String,sellerType:Int
                   ,gmtModified:Long,packingValue:Int,carPartsType:Int,packageMeasureUnit:String,goodsQualityType:Int,
                    goodsThirdQualityType:String,subName:String,vulgo:String,outerPackingSize:String,
                    outerPackingForm:String,seriesId:Int,goodsTotalWeight:String,goodsSize:String,
                    buyMinNumber:Int,buyStepLength :Int,spuId :Int,eclpGoodsSn:String,
                    isSynEclp:Int,warehouseFlag:Int,sellerGoodsCode:String,sellergoodsName:String,standardCode:String,
                    goodsBizType:String,factoryId:Int,cuCode:String,basePrice:String,originKey:String,
                    upgradeStatus:Int)
object sqlAnalysisToEs {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

   val data = env.addSource(new MyJdbcsink())
    data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DbGoods](Time.seconds(0L)) {
      override def extractTimestamp(element: DbGoods): Long = {
        element.addTime
      }
    }).setParallelism(1)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val ecommerceLogTable = tableEnv.fromDataStream(data)
    val table:Table =ecommerceLogTable.select("das,asd").filter("as=11")


  }


}
class MyJdbcsink() extends RichSourceFunction[DbGoods]{
  var conn: Connection = _
  var select: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn=DriverManager.getConnection("jdbc:mysql://172.28.48.96:80/autoparts",
    "canal",
    "tk7U3pGhK")
    conn.prepareStatement("select * from db_goods")
  }


  override def run(sourceContext: SourceFunction.SourceContext[DbGoods]): Unit = {

  }

  override def cancel(): Unit = ???

  override def close(): Unit = {
    select.close()
  }



}