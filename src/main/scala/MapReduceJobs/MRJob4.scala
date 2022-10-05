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
        val patternVal = Constants.mainPattern.findFirstIn(value.toString)
        patternVal match
          case Some(valueVal) =>
            val readAndSplit = value.toString.split(" ")
            val messageLevel = readAndSplit(2)
            if (messageLevel.equals(Constants.messageLevelError) || messageLevel.equals(Constants.messageLevelDebug)) {
              logger.info("message Level " + messageLevel + " The pattern from logger Val" + readAndSplit(5))
              word.set(messageLevel)
              output.collect(word, new Text(readAndSplit(5)))
            } else if (messageLevel.equals(Constants.messageLeveInfo) || messageLevel.equals(Constants.messageLevelWarn)) {
              logger.info("message Level " + messageLevel + " The pattern from logger Val " + readAndSplit(6))
              word.set(messageLevel)
              output.collect(word, new Text(readAndSplit(6)))
            } else {
              logger.info("Message level match not found")
            }
          case None => None
      } catch {
        case e: Exception => logger.trace("The Exception Occurred is" + e.toString)
      }
      logger.info("*******************Exiting MaxLengthMapper-Map****************")
  
  class MaxLengthReducer extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MaxLengthReducer-Reduce****************")
      val maxValue = values.asScala.reduce((valueOne, valueTwo) => {
        if(valueOne.getLength > valueTwo.getLength){
          valueOne
        }else{
          valueTwo
        }
      })
      val outputValue = new IntWritable(maxValue.getLength)
      logger.info("The outputValue is " + outputValue)
      output.collect(new Text(key.toString + ","+ maxValue.toString), outputValue)
      logger.info("*******************Exiting MaxLengthReducer-Reduce****************")
}
