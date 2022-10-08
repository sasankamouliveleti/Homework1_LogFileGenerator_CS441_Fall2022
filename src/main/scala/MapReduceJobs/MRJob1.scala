package MapReduceJobs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import HelperUtils.{CreateLogger, Constants}
import com.typesafe.config.ConfigFactory
import org.slf4j.Logger


object MRJob1 {
  val logger : Logger = CreateLogger(classOf[TimeTypeMapper]) /* Defining the logger of TimeTypeMapper*/

  /* The Goal of this Mapper Class is to generate Key's of type Text which are the (hh:mm hh:mm Message Level)
  and the Values of type Intwritable which here specifies 1 as we are just 
  mapping the each time interval, message level in each log line which matches the regex to 1*/
  class TimeTypeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1) /* Defining the value to be 1*/
    private val word = new Text() /*Defining the word parameter where we will store the key of the mapper*/
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        /* Fetching the user defined search pattern from Constants(which gets it from app.conf) and comparing with the input line*/
        val patternVal = Constants.mainPattern.findFirstIn(value.toString)
        /* for each result case of above statement*/
        patternVal match
          /* Case when there is a match*/
          case Some(valueVal) =>
            val readAndSplit = value.toString.split(" ") /* Split the input line with space*/
            val logTimeStamp = readAndSplit(0) /* The first element would be the logTimeStamp*/
            val messageLevel = readAndSplit(2) /* The third element would be the message level*/
            val timeSplit = logTimeStamp.split(":") /* split the time stamp to get hours and minutes*/
            logger.info("The time split is" + timeSplit(0) + timeSplit(1))
            /* Generating the map key for the mapper as hh:mm hh:mm messagelevel*/
            val mapkey = Constants.generateTimeInterval(timeSplit) + " " + messageLevel
            word.set(mapkey)
            /* Setting the key and value to the context output*/
            output.collect(word, one)
          /* For case when there is no match do nothing */
          case None => None
      } catch {
        case e: Exception => logger.debug("The exception occurred is" + e.toString)
      }
      logger.info("*******************Exiting TimeTypeMapper-Map****************")

  /* The Goal of this Reducer is to take the TypeTypeReducer mapper key values of format (hh:mm hh:mm message level):[1,1,1,1]
  and reduce them to (hh:mm hh:mm message level):4*/
  class TimeTypeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[TimeTypeReducer])
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeReducer-Reduce****************")
      /* Reduces by suming all the values in the values iterator*/
      val sum = values.asScala.reduce(
        (valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      /* Output the key(hh:mm hh:mm message level) and value(sum)* to the context*/
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting TimeTypeReducer-Reduce****************")
}
