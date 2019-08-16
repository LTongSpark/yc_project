package com.zytc.yc.spark.seneitiveInfo

import com.zytc.yc.spark.seneitiveInfo.analyse._
import com.zytc.yc.spark.util.{KafkaUtil, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 操作类型	删除操作	    是否存在			                    安全小组，应用管理员
  * DDL操作	            是否存在			                    安全小组，应用管理员
  * 次数	查询/导出次数异常	动态阈值	    天		                安全小组，应用管理员
  * 短时间内插入次数异常	动态阈值			                    安全小组，应用管理员
  * 数量	查询/导出数据量异常	动态阈值	    天、周、月、单次	        安全小组，应用管理员
  * 关键对象更新操作	    是否存在			                    安全小组，应用管理员
  *
  */
object SensitiveInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        //获取Config
        val config = Utility.parseConfFile(args(0))
        val conf = new SparkConf().setAppName("SensitiveInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(config.getInt("kafka.batch_period")))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(config, ssc, 2)

        //次数查询/导出次数异常
        CountSelectAbnormal.countSelect(ssc, config, streamData)
        //短时间
        ShortTimeAbnomal.shortTime(ssc, config, streamData)
        //查询导出数据异常
        AffectAbnormal.affect(config, streamData)
        //删除操作是否存在
        DeleteNum.delete(config, streamData)
        //ddl操作是否存在
        DDLNum.ddl(config, streamData)
        //ddl操作是否存在
        ApiLog.api(config,streamData)
        ssc.start()
        ssc.awaitTermination()
    }
}