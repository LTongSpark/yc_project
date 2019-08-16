package com.zytc.yc.spark.seneitiveInfo

import java.sql.DriverManager
import java.util.logging.Logger

import com.zytc.yc.spark.Repository.ViolationsRepository
import com.zytc.yc.spark.config.GlobalConfig
import com.zytc.yc.spark.loginInfo.LoadBean
import com.zytc.yc.spark.util.Utility
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NotLoginTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("kafka")
        conf.setMaster("local[*]")
        val sc = new SparkContext(conf)
        val text =sc
    }
    def createConn() = {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://localhost:3306/ycdata", "root", "root")
    }
}
