package HelperUtils

import MapReduceJobs.MRJob1.logger
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.matching.Regex

object Constants {

  val messageLeveInfo = "INFO" /* Defining constants for INFO*/
  val messageLevelError = "ERROR"/* Defining constants for ERROR*/
  val messageLevelDebug = "DEBUG" /* Defining constants for DEBUG*/
  val messageLevelWarn = "WARN" /* Defining constants for WARN*/

  /* Defining various Job Names*/
  val MRJob1 = "MessageLevelFrequencyInTimeIntervals"
  val MRJob2 = "SortedErrorLevelLogs"
  val MRJob2_Final = "DescSortErrorTypes"
  val MRJob3 = "MessageLevelFrequency"
  val MRJob4 = "LongestMatchLog"

  val fileSystemType =  "fs.defaultFS"
  val fileSystemTypeVal = "file:///"
  val noOfMappers = "mapreduce.job.maps"
  val noOfReducers = "mapreduce.job.reduces"
  val noOfMappersVal = "1"
  val noOfReducersVal = "1"

  /* Defining place holder values*/
  val output0 = "_ascsort"
  val escapeTab = "\t"
  val space = " "

  /* Getting user defined attributes from config file*/
  val config: Config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")
  val mainPattern: Regex = Constants.config.getString("patternToSearch").r
  val definedTimeInterval: Int = config.getInt("timeInterval")

  /* This method converts give string if not in a time format of hh:mm to correct format*/
  def convertToTimeStampInterval(timeValue: String): String ={
    val split = timeValue.split(":")
    if(split(0).length == 1){
      if(split(1).length == 1){
        "0"+split(0) + ":" + "0" + split(1)
      } else{
        "0"+split(0) + ":" + split(1)
      }
    }else {
      if (split(1).length == 1) {
        split(0) + ":" + "0" + split(1)
      } else {
        split(0) + ":" + split(1)
      }
    }
  }

  /* This method returns the correct time interval where a given input time has to be placed*/
  def generateTimeInterval(timeSplit: Array[String]): String = {
    logger.debug("the time interval set by user is " + definedTimeInterval.toString)
    val hours = timeSplit(0).toInt
    val lowerBoundMin = (timeSplit(1).toInt / (definedTimeInterval%60)) * (definedTimeInterval%60)
    val upperBoundMin = lowerBoundMin + (definedTimeInterval%60)
    if(lowerBoundMin == 0 && upperBoundMin == 60){
      convertToTimeStampInterval((hours%24).toString + ":" + "00") + " " + convertToTimeStampInterval(((hours + 1)%24).toString + ":" + "00")
    }
    else if (upperBoundMin == 60) {
      convertToTimeStampInterval((hours%24).toString + ":" + lowerBoundMin.toString) + " " + convertToTimeStampInterval(((hours + 1)%24).toString + ":" + "00")
    } else if (upperBoundMin > 60) {
      convertToTimeStampInterval((hours%24).toString + ":" + lowerBoundMin.toString) + " " + convertToTimeStampInterval(((hours + 1)%24).toString + ":" + (upperBoundMin - 60).toString)
    } else {
      convertToTimeStampInterval((hours%24).toString + ":" + lowerBoundMin.toString) + " " + convertToTimeStampInterval((hours%24).toString + ":" + upperBoundMin.toString)
    }
  }
  
  /* Method to sort a give list of tuples based on second element value*/
  def sortBasedOnSecondVal(arr: List[(String, Int)]): Array[(String, Int)] ={
    /*Reference - https://stackoverflow.com/questions/2627919/scala-how-can-i-sort-an-array-of-tuples-by-their-second-element*/
    scala.util.Sorting.stableSort(arr, (e1: (String, Int), e2: (String , Int))=> e1._2 < e2._2)
  }
}
