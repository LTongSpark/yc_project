package com.zytc.yc.spark.authLogInfo

import com.zytc.yc.spark.authLogInfo.analyse._
import com.zytc.yc.spark.util.{KafkaUtil, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 1,非工作时间操作                          定值
  * 2，关键对象授权异常                        白名单
  * 3，短时间内相同权限授予并收回               是否存在  一天存在
  * 4，非域账号                              白名单
  * 5， DB/OS用户变化                        白名单
  */
object AuthLogInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        //获取Config
        val config = Utility.parseConfFile(args(0))
        val conf = new SparkConf().setAppName("authLogInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(config.getInt("kafka.batch_period")))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(config, ssc, 3)

        //非工作时间授权
        NotWorkOper.notWorkOper(ssc, streamData, config)
        //短时间内相同权限授予并收回
        SamePer.samePer(streamData, config)

        //关键对象授权异常
        //KeyObject.key(config, streamData, mysqlStatisticsUrl)
        //非域账号
        //NotAreaAccount.notArea(config, streamData, mysqlStatisticsUrl)
        //DB/OS变化
        //DSChange.DS(config, streamData, mysqlStatisticsUrl)
        ssc.start()
        ssc.awaitTermination()
    }
}
