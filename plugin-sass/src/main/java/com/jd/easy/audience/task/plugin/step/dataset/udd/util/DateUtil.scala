package com.jd.easy.audience.task.plugin.step.dataset.udd.util

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.task.plugin.property.ConfigProperties

import scala.collection.mutable.ArrayBuffer

object DateUtil {

  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_FORMAT_NO_BAR = "yyyyMMdd"
  val DAY_MS: Int = 24 * 3600 * 1000

  final val configProperties = new ConfigProperties()

  def getDateAsString(d: Date): String = {
    getDateAsString(d, DATE_FORMAT)
  }

  def getDateAsString(d: Date, format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.format(d)
  }

  def convertStringToDate(s: String): Date = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.parse(s)
  }

  def convertIntToDate(date: Int): Date = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT_NO_BAR)
    dateFormat.parse(date.toString)
  }

  def getDateBeforeNDays(dt: Date, interval: Int): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);
    cal.add(Calendar.DATE, -interval)
    cal.getTime
  }

  def getDateStrBeforeNDays(dt: Date, interval: Int): String = {
    getDateAsString(getDateBeforeNDays(dt, interval))
  }

  def getDateStrNextNDays(dt: String, interval: Int): String = {
    val date = convertStringToDate(dt)
    getDateStrBeforeNDays(date, -interval)
  }

  def getDateIntNextNDays(dateInt: Int, interval: Int): Int = {
    val date = convertIntToDate(dateInt)
    getDateStrBeforeNDays(date, -interval).replaceAll("-",  "").toInt
  }

  def getDiffDays(start: String, end: String): Int = {
    val startDate = convertStringToDate(start);
    val endDate = convertStringToDate(end);
    val diffMs = endDate.getTime - startDate.getTime;
    (diffMs / DAY_MS).toInt
  }

  def getYear(dateStr: String): String = {
    dateStr.split("-")(0)
  }

  def getMonth(dateStr: String): String = {
    dateStr.split("-")(1)
  }

  def getDay(dateStr: String): String = {
    dateStr.split("-")(2)
  }

  def getWeek(dateStr: String): String = {
    val date = convertStringToDate(dateStr)
    val cal: Calendar = Calendar.getInstance()
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.setTime(date);
    val nth = cal.get(Calendar.WEEK_OF_YEAR)
    if (nth < 10) {
      "0" + nth.toString
    } else {
      nth.toString
    }
  }

  def getNowDate: String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(DATE_FORMAT)
    val date = dateFormat.format(now)
    date
  }

  def getLogical(startDateStr: String, endDateStr: String): Int = {
    val diffDays = DateUtil.getDiffDays(startDateStr, endDateStr)
    if (diffDays < 1) {
      DateUtil.getDateAsString(DateUtil.convertStringToDate(startDateStr), "yyyyMMdd").toInt
    } else if (diffDays < 7) {
      ("99" + DateUtil.getYear(endDateStr) + DateUtil.getWeek(endDateStr)).toInt
    } else {
      (DateUtil.getYear(startDateStr) + DateUtil.getMonth(startDateStr)).toInt
    }
  }

  def getAllMonthDate(dateStr: String): Seq[String] = {
    val dates = ArrayBuffer[String]()
    val c = Calendar.getInstance
    val year = Integer.parseInt(dateStr.split("-")(0))
    val month = Integer.parseInt(dateStr.split("-")(1))
    val month_str = if (month < 10) "0" + month.toString else month.toString
    c.set(Calendar.YEAR, year);
    c.set(Calendar.MONTH, month - 1);
    val maxDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
    for (i <- 1 to maxDay) {
      if (i < 10) {
        dates.append( s"""$year-$month_str-0$i""")
      } else {
        dates.append( s"""$year-$month_str-$i""")
      }
    }
    dates.toSeq
  }

  def getLastDayOfMonth(dateStr: String): String = {
    val year = Integer.parseInt(dateStr.split("-")(0))
    val month = Integer.parseInt(dateStr.split("-")(1))
    val month_str = if (month < 10) "0" + month.toString else month.toString
    val c = Calendar.getInstance
    c.set(Calendar.YEAR, year);
    c.set(Calendar.MONTH, month - 1);
    val maxDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
    s"""$year-$month_str-$maxDay"""
  }

  def getFirstDayOfMonth(dateStr: String): String = {
    val year = Integer.parseInt(dateStr.split("-")(0))
    val month = Integer.parseInt(dateStr.split("-")(1))
    val month_str = if (month < 10) "0" + month.toString else month.toString
    s"""$year-$month_str-01"""
  }

  def getLastDayOfLastWeek(): String = {
    val cal: Calendar = Calendar.getInstance();
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    val date = df.format(cal.getTime())
    date
  }

  def getDayOrderOfWeek(dateStr: String): Int = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(DateUtil.convertStringToDate(dateStr))
    cal.get(Calendar.DAY_OF_WEEK)
  }

  def getDayOrderOfMonth(dateStr: String): Int = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(DateUtil.convertStringToDate(dateStr))
    cal.get(Calendar.DAY_OF_MONTH)
  }

  def getCurrentTimeStamp(): Long = {
    val now = new Date()
    now.getTime
  }

  def getBeforeNMonthFirstDate(dt: String, nMonths: Int): String = {
    val monthStr = dt.split("-")(1)
    val yearStr = dt.split("-")(0)
    var year = 0
    var month = 0
    var yearPlus = 0
    if (monthStr.toInt - nMonths > 0 && monthStr.toInt - nMonths <= 12) {
      year = yearStr.toInt
      month = monthStr.toInt - nMonths
    } else if (monthStr.toInt - nMonths <= 0) {
      yearPlus = (nMonths-monthStr.toInt)/ 12+1
      year = yearStr.toInt - yearPlus
      month = 12 + (monthStr.toInt - nMonths) % 12
    } else {
      yearPlus = (monthStr.toInt - nMonths) / 12
      year = yearStr.toInt + yearPlus
      month = (monthStr.toInt - nMonths) % 12
    }
    if(month==0)
      month=12
    if (month < 10) {
      s"""$year-0$month-01"""
    } else {
      s"""$year-$month-01"""
    }
  }

  def getWeekEndStr(dateStr: String): String = {
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.setTime(DateUtil.convertStringToDate(dateStr))
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    cal.add(Calendar.WEEK_OF_YEAR, 1)
    df.format(cal.getTime)
  }

  def getLogicDt(dateStr: String, dateEnd: String, period: String): Long = {
    val WEEK_PERIOD = ConfigProperties.WEEK_PERIOD
    val MONTH_PERIOD = ConfigProperties.MONTH_PERIOD
    val RECENT_7DAY_PERIOD = ConfigProperties.RECENT_7DAY_PERIOD
    val RECENT_30DAY_PERIOD = ConfigProperties.RECENT_30DAY_PERIOD
    val DAY_PERIOD = ConfigProperties.DAY_PERIOD
    period match {
      case WEEK_PERIOD => ("99" + DateUtil.getYear(dateEnd) + DateUtil.getWeek(dateEnd)).toLong
      case MONTH_PERIOD => (DateUtil.getYear(dateStr) + DateUtil.getMonth(dateStr)).toLong
      case RECENT_7DAY_PERIOD => ("87" + DateUtil.getYear(dateStr) + DateUtil.getMonth(dateStr) + DateUtil.getDay(dateStr)).toLong
      case RECENT_30DAY_PERIOD => ("88" + DateUtil.getYear(dateStr) + DateUtil.getMonth(dateStr) + DateUtil.getDay(dateStr)).toLong
      case DAY_PERIOD => DateUtil.getDateAsString(DateUtil.convertStringToDate(dateStr), "yyyyMMdd").toLong
    }
  }

  def getDateStrHis(dateStr: String, period: String): String  = {
    period match {
      case ConfigProperties.WEEK_PERIOD => DateUtil.getDateStrBeforeNDays(DateUtil.convertStringToDate(dateStr), 6)
      case ConfigProperties.MONTH_PERIOD => DateUtil.getFirstDayOfMonth(dateStr)
      case ConfigProperties.RECENT_7DAY_PERIOD => DateUtil.getDateStrBeforeNDays(DateUtil.convertStringToDate(dateStr), 6)
      case ConfigProperties.RECENT_30DAY_PERIOD => DateUtil.getDateStrBeforeNDays(DateUtil.convertStringToDate(dateStr), 29)

    }
  }

  def getDateAfterNDays(dt: Date, interval: Int): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, interval)
    cal.getTime
  }

  /**
    * 获取时间周期列表
    *
    * @param period
    * @param dateStart
    * @param dateEnd
    * @return
    */
  def getTimeList(period: Int, dateStart: String, dateEnd: String): collection.mutable.ListBuffer[(Int, Int)] = {
    var timeList = new collection.mutable.ListBuffer[(Int, Int)]()
    val endDt = DateUtil.convertStringToDate(dateEnd)
    var nowDt = DateUtil.convertStringToDate(dateStart)
    while (nowDt.getTime <= endDt.getTime) {
      var start = 20131231
      if (period != 0) {
        start = DateUtil.getDateAsString(DateUtil.getDateBeforeNDays(nowDt, period)).replace("-", "").toInt
      }
      val end = DateUtil.getDateAsString(nowDt).replace("-", "").toInt
      timeList.+=((start, end))
      nowDt = DateUtil.getDateAfterNDays(nowDt, 1)
    }
    return timeList
  }

  /**
    * @param date 日期是否所属当前月
    * @return
    */
  def dateIsInCurrentMonth(date: String): Boolean = {
    val dateParam = LocalDate.parse(date, DateTimeFormatter.ofPattern(DATE_FORMAT))
    val now = LocalDate.now()
    dateParam.getYear == now.getYear && dateParam.getMonth == now.getMonth
  }

  def getQuarterFirstDay(dateStr: String): String = {
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.setTime(DateUtil.convertStringToDate(dateStr))
    cal.set(Calendar.DAY_OF_MONTH, 1)

    val currentMonth = cal.get(Calendar.MONTH) + 1
    try {
      if (currentMonth >= 1 && currentMonth <= 3) {
        cal.set(Calendar.MONTH, 0)
      }
      else if (currentMonth >= 4 && currentMonth <= 6) {
        cal.set(Calendar.MONTH, 3)
      }
      else if (currentMonth >= 7 && currentMonth <= 9) {
        cal.set(Calendar.MONTH, 6)
      }
      else if (currentMonth >= 10 && currentMonth <= 12) {
        cal.set(Calendar.MONTH, 9)
      }
    } catch {
      case e: Exception =>
        throw new JobInterruptedException("日期解析异常", "DateUtil occurs error！")
    }
    df.format(cal.getTime)
  }

  def getWeekFirstDay(dateStr: String): String = {
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.setTime(DateUtil.convertStringToDate(DateUtil.getDateStrNextNDays(dateStr,-1)))
    cal.set(Calendar.DAY_OF_WEEK, 2)
    df.format(cal.getTime)
  }

}
