package com.zytc.yc.spark.seneitiveInfo

import com.zytc.yc.spark.config.GlobalConfig
import com.zytc.yc.spark.loginInfo.LoadBean
import com.zytc.yc.spark.util.Utility
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountSelectAbnornalTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("kafka")
        conf.setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(30))
        val streamData = ssc.socketTextStream("192.168.112.101", 9999)
        val config = Utility.parseConfFile("D:\\mobsf\\yc\\src\\main\\resources\\log_sta.conf")
        var pcMap: Map[String, Int] = Map()
        //获取系统阈值
        val thresholdValue: Int = GlobalConfig.threshold.getOrElse("DayLoginFail", 10)
        val json = streamData
            .map(rdd => LoadBean.loadLogBean(rdd))
            .filter(_.base_userId.length > 0)
            .map(x => {
                (x.operation_act_result, (
                    x.base_userId, x.interfaceNo, x.infotype,
                    x.base_userType, x.base_hostname, x.base_client,
                    x.base_ip, x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                    x.operation_type, x.operation_abnormal,
                    x.operation_act_do,
                    x.optional_dataClass, x.optional_tablename, x.optional_databasename,
                    x.optional_fieldname, x.origin_content))
            })
            .window(Seconds(120) ,Seconds(30))
        println("biz_log_DayLoginFail" + "解析完成")

        val fail = List("fail")
        val key_object = ssc.sparkContext.parallelize(fail)
            .map(x => (x, true))

        json.transform(x => {
            x.leftOuterJoin(key_object)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => {
                var dataClass = 0
                if (x._2._1._14 == "") dataClass = 0
                else dataClass = x._2._1._14.toInt
                (x._2._1._1, x._1, x._2._1._2, x._2._1._3, x._2._1._4,
                    x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9,
                    x._2._1._10, x._2._1._11, x._2._1._12, x._2._1._13, dataClass,
                    x._2._1._15, x._2._1._16, x._2._1._17, x._2._1._18)
            })
            .map(x => {
                var isViolations = 0 //是否违规(超出阀值)
                var currentNum: Int = pcMap.getOrElse(x._1, 0) + 1;
                pcMap = pcMap.updated(x._1, currentNum);
                //判断是否违规
                if (currentNum > thresholdValue) isViolations = 1
                (x._2, (x._1, x._3, x._4, x._5, x._6, x._7,
                    x._8, x._9, x._10, x._11, x._12, x._13,
                    x._14, x._15, x._16, x._17, x._18, x._19,
                    currentNum, isViolations))

            })
            //处理下一步的并发
            .transform(x => {
            x.leftOuterJoin(key_object, 4)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => (x._2._1._1, x._1, x._2._1._2, x._2._1._3, x._2._1._4,
                x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9,
                x._2._1._10, x._2._1._11, x._2._1._12, x._2._1._13, x._2._1._14,
                x._2._1._15, x._2._1._16, x._2._1._17, x._2._1._18, x._2._1._19, x._2._1._20
            )).map(x => {
            //获取历史记录
//            val userTuple2 = ViolationsRepository.Repository.get("DayLoginFail").get.getOrElse(x._1, Tuple2(0, "")) //(累计次数,最后一次操作时间)
//            if (x._20 <= userTuple2._1 || x._9 <= userTuple2._2) { //不要的
//                if (x._9 == userTuple2._2) {
//                    //删掉历史
//                    var historyMap = ViolationsRepository.Repository.get("DayLoginFail").get.updated(x._1, (0, x._9))
//                    ViolationsRepository.Repository = ViolationsRepository.Repository.updated("DayLoginFail", historyMap)
//                }
//                Row(Nil)
//            } else {
//                //更新次数为当前次数,时间更新为本条日志的timestamp
//                var historyMap = ViolationsRepository.Repository.get("DayLoginFail").get.updated(x._1, (x._20, x._9))
//                ViolationsRepository.Repository = ViolationsRepository.Repository.updated("DayLoginFail", historyMap)
//                Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11,
//                    x._12, x._13, x._14, x._15, x._16, x._17, x._18, x._19, x._20, x._21)
//            }
        }).filter(!_.equals(Row(Nil)))
               .print()
        ssc.start()
        ssc.awaitTermination()
    }


}
