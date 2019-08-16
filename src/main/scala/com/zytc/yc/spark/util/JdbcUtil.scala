package com.zytc.yc.spark.util

/**
  * 数据库驱动程序
  */

import java.sql.DriverManager
import com.typesafe.config.Config

object JdbcUtil {
    /**
      * 链接数据库
      * @param config
      * @return
      */
    def createConn(config: Config) = {
        val url = config.getString("mysql.url")
        val username = config.getString("mysql.username")
        val password = config.getString("mysql.password")
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(url, username, password)
    }

}
