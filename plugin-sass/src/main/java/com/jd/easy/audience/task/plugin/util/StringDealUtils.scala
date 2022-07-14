package com.jd.easy.audience.task.plugin.util

object StringDealUtils extends Serializable {

  /**
   * 解析分隔符
   *
   * @param fieldDelimter
   * @return
   */
  def  parseFieldDelimter(fieldDelimiter: String) = {
    val replaceChar = "\\\\"
    val charSpecial= Array("$","(", ")","*","+",".","[","]","?","\\","/","^","{","}","|")
    val charArr = fieldDelimiter.toCharArray
    val fieldDelimiterNew = charArr.map {
      char =>
        if (charSpecial.contains(char.toString)) {
          replaceChar + char.toString
        } else {
          char.toString
        }
    }.mkString("")
    fieldDelimiterNew
  }

}
