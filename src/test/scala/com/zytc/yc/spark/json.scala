package com.zytc.yc.spark

import com.zytc.yc.spark.util.{Utility, ParseUserId}

object json {
    def partition: PartialFunction[String, Int] = {
        case "one" => {
            1
        }
        case "two" => {
            2
        }
        case "tong" => {
            3
        }
    }

    def main(args: Array[String]): Unit = {
        var str = "123:123"

//        print(str.contains(":"))
//        print(str.substring(0, str.indexOf(":")))
        print(parse(str))
    }

    def parse(userid: String): Tuple2[String, String] = {
        var userId = ""
        var sendTo: String = ""
        if (userid.equals("")) userId = userid
        else if (userid.contains(":")) {
            userId = userid.substring(0, userid.indexOf(":"))
            sendTo = userid.substring(userid.indexOf(":") + 1)
        }
        else if (!userid.contains(",")) userId = userid
        else {
            userId = userid.substring(0, userid.indexOf(","))
            sendTo = userid.substring(userid.indexOf(",") + 1)
        }
        (userId, sendTo)
    }
}