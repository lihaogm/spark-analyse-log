package com.lihaogn.spark

import java.sql.DriverManager

/**
  * 通过jdbc方式访问
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn=DriverManager.getConnection("jdbc:hive2://localhost:14000","mac","")
    val pstmt=conn.prepareStatement("select name,age,score from student")
    val rs=pstmt.executeQuery()
    while (rs.next()){
      println("name: "+rs.getString("name")+
      ", age: "+rs.getInt("age")+
      ", score: "+rs.getDouble("score"))
    }

    rs.close()
    pstmt.close()
    conn.close()
  }
}
