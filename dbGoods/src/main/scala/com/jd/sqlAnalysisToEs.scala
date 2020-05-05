package com.jd

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mysql.cj.protocol.Resultset
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

case class  DbGoods(goodId:Integer,catId:Int,goodsSn:String,
                    newGoodsSn:String ,goodsName:String,BrandId:String,
                    goodsNumber:String,goodsWeight:String,goorsBrief:String,
                    goodsThumb:String,goodsImg:String,addTime :Int,
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

    env.addSource(new MyJdbcsink())


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
class MyJdbcSource extends RichSourceFunction[DbGoods]{
  var connection: Connection = _
  var ps: PreparedStatement = _

  val userName="canal"
  val password="tk7U3pGhK"
  val url="jdbc:mysql://172.28.48.96:80/autoparts"
  val driver = "com.mysql.jdbc.Driver"
  val sql="select * from db_goods"

  @throws(classOf[Exception])
  override def open(parameters: Configuration) : Unit = {
    super.open(parameters)
    Class.forName(driver)

      connection = DriverManager.getConnection(url)
      ps = connection.prepareStatement(sql)

  }
  @throws(classOf[Exception])
  override def run(sourceContext: SourceFunction.SourceContext[DbGoods]): Unit = {
    var resultset:Resultset=ps.executeQuery()
    while(resultset.next()){
     var dbGoods :DbGoods=new DbGoods(resultset.getString("goods_id"),
       resultset.getString("cat_id"),resultset.getString("goods_sn"),
       resultset.getString("goods_sn"),resultset.getString("goods_sn"),
       resultset.getString("goods_sn"),resultset.getString("goods_sn"),
       resultset.getString("goods_sn"),resultset.getString("goods_sn")
     )

    }

  }

  override def cancel(): Unit = ???
}