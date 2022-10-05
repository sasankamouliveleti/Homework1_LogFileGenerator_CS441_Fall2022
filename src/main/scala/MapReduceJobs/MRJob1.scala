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
  val logger : Logger = CreateLogger(classOf[TimeTypeMapper])
  class TimeTypeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        val patternVal = Constants.mainPattern.findFirstIn(value.toString)
        patternVal match
          case Some(valueVal) =>
            val readAndSplit = value.toString.split(" ")
            val logTimeStamp = readAndSplit(0)
            val messageLevel = readAndSplit(2)
            val timeSplit = logTimeStamp.split(":")
            logger.info("The time split is" + timeSplit(0) + timeSplit(1))
            val mapkey = Constants.generateTimeInterval(timeSplit) + " " + messageLevel
            word.set(mapkey)
            output.collect(word, one)
          case None => None
      } catch {
        case e: Exception => logger.trace("The exception occurred is" + e.toString)
      }
      logger.info("*******************Exiting TimeTypeMapper-Map****************")

  class TimeTypeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[TimeTypeReducer])
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeReducer-Reduce****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting TimeTypeReducer-Reduce****************")
}
