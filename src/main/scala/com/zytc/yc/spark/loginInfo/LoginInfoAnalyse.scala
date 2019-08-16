package com.zytc.yc.spark.loginInfo

import com.zytc.yc.spark.loginInfo.analyse._
import com.zytc.yc.spark.util.{KafkaUtil, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 通过加载json文件分析连续失败次数，一天内登录失败次数多，一小时内登录失败次数多，10分钟内登录次数多
  */

object LoginInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        //获取Config
        val config = Utility.parseConfFile(args(0))
        val conf = new SparkConf().setAppName("loginInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(config.getInt("kafka.batch_period")))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(config, ssc, 1)

        //抽取出需要的数据并判断是否为业务部门的数据
        NotWorkLogin.notWorkLogin(ssc, streamData, config)
        //离职后登陆
        QuitLogin.quitLogin(config, streamData)
        //一天内登录的失败的次数
        DayLoginFail.loginFair(ssc, config, streamData)
        //一个小时内登录的失败次数
        HourLoginFail.loginFair(ssc, config, streamData)
        //10分钟登录的次数
        MinLogFail.minLogFail(ssc, config, streamData)
        //连续失败的次数
        FreqFail.freqFail(ssc, config, streamData)
        //启动流
        ssc.start()
        //等待接待
        ssc.awaitTermination()
    }
}
