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
import org.slf4j.Logger
import com.typesafe.config.ConfigFactory

object MRJob4 {
  val logger : Logger = CreateLogger(classOf[MaxLengthMapper])
  class MaxLengthMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      logger.info("*******************Entering MaxLengthMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        val readAndSplit = value.toString.split(" ")
        val messageLevel = readAndSplit(2)
        if (messageLevel.equals(Constants.messageLevelError) || messageLevel.equals(Constants.messageLevelDebug)) {
          val matchedRegexStr = Constants.mainPattern.findAllMatchIn(readAndSplit(5))
          matchedRegexStr.foreach(eachItem =>{
            logger.info("message Level " + messageLevel + " The pattern from logger eachItem Val" + eachItem)
            word.set(messageLevel)
            output.collect(word, new Text(eachItem.toString()))
          })
        } else if (messageLevel.equals(Constants.messageLeveInfo) || messageLevel.equals(Constants.messageLevelWarn)) {
          val matchedRegexStr = Constants.mainPattern.findAllMatchIn(readAndSplit(6))
          matchedRegexStr.foreach(eachItem => {
            logger.info("message Level " + messageLevel + " The pattern from logger eachItem Val" + eachItem)
            word.set(messageLevel)
            output.collect(word, new Text(eachItem.toString()))
          })
        } else {
          logger.info("Message level match not found")
        }
      } catch {
        case e: Exception => logger.trace("The Exception Occurred is" + e.toString)
      }
      logger.info("*******************Exiting MaxLengthMapper-Map****************")
  
  class MaxLengthReducer extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MaxLengthReducer-Reduce****************")
      val valuesFound = values.asScala.map(value =>{
        (value.toString,value.toString.length)
      }).toList
      /*https://stackoverflow.com/questions/2627919/scala-how-can-i-sort-an-array-of-tuples-by-their-second-element*/
      val sortedValues = scala.util.Sorting.stableSort(valuesFound, (e1: (String, Int), e2: (String , Int))=> e1._2 < e2._2)
      output.collect(new Text(key.toString + ","+ sortedValues.last(0)), new IntWritable(sortedValues.last(1)))
      logger.info("*******************Exiting MaxLengthReducer-Reduce****************")
}
