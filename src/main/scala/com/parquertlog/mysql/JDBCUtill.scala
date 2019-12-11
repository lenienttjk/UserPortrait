package com.parquertlog.mysql

import java.sql.{Connection, DriverManager}

object JDBCUtill {
  val driver = "com.mysql.jdbc.Driver"
  val dbUrl = "jdbc:mysql://localhost:3307/userpaint?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC"
  val userName = "root"
  val password = "123456"

  var connection: Connection = null

  def getConnection = {
    Class.forName(driver)
    connection = DriverManager.getConnection(JDBCUtill.dbUrl, JDBCUtill.userName, JDBCUtill.password)
    connection
  }

}