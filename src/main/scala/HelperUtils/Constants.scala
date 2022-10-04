package HelperUtils

import MapReduceJobs.MRJob1.logger
import com.typesafe.config.ConfigFactory

object Constants {
  val messageLeveInfo = "INFO"
  val messageLevelError = "ERROR"
  val messageLevelDebug = "DEBUG"
  val messageLevelWarn = "WARN"

  val MRJob1 = "MessageLevelFrequencyInTimeIntervals"
  val MRJob2 = "SortedErrorLevelLogs"
  val MRJob3 = "MessageLevelFrequency"
  val MRJob4 = "LongestMatchLog"

  private val config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")

  def generateTimeInterval(timeSplit: Array[String]): String = {
    val definedTimeInterval = config.getInt("timeInterval")
    logger.debug("the time interval set by user is" + definedTimeInterval.toString)
    val hours = timeSplit(0).toInt
    val lowerBoundMin = (timeSplit(1).toInt / definedTimeInterval) * definedTimeInterval
    val upperBoundMin = lowerBoundMin + definedTimeInterval
    if (upperBoundMin == 60) {
      (hours + 1).toString + ":" + lowerBoundMin.toString + " " + (hours + 1).toString + ":" + 00.toString
    } else if (upperBoundMin > 60) {
      (hours + 1).toString + ":" + lowerBoundMin.toString + " " + (hours + 1).toString + ":" + (upperBoundMin - 60).toString
    } else {
      hours.toString + ":" + lowerBoundMin.toString + " " + hours.toString + ":" + upperBoundMin.toString
    }
  }
}
