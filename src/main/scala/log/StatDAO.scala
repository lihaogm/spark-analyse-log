package log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的DAO操作
  */
object StatDAO {

  /**
    * 批量保存DayVedioAccessStat到数据库
    */
  def insertDayVedioAccessTopN(list: ListBuffer[DayVedioAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection() // 获取数据库连接
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_vedio_access_topn_stat(day,class_id,times) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.classId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() // 手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }

  /**
    * 批量保存DayCityVideoAccessStat到数据库
    */
  def insertDayCityVedioAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection() // 获取数据库连接
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_vedio_city_access_topn_stat(day,class_id,city,times,times_rank) values(?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.classId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() // 手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }

  /**
    * 批量保存 DayTrafficVideoAccessStat 到数据库
    */
  def insertDayVedioTrafficsAccessTopN(list: ListBuffer[DayTrafficVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection() // 获取数据库连接
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_vedio_traffics_topn_stat(day,class_id,traffics) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.classId)
        pstmt.setLong(3, ele.traffics)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() // 手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }

  /**
    * 删除指定日期的数据
    */
  def deleteData(day: String): Unit = {

    val tables = Array("day_vedio_access_topn_stat",
      "day_vedio_city_access_topn_stat",
      "day_vedio_traffics_topn_stat")

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MySqlUtils.getConnection()
      for (table <- tables) {
        val deleteSQL = s"delete from $table where day=?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }

  }

}
