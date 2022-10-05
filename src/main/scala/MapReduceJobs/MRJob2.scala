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


object MRJob2 {
  val logger : Logger = CreateLogger(classOf[ErrorCounterMapper])
  class ErrorCounterMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        val patternVal = Constants.mainPattern.findFirstIn(value.toString)
        patternVal match
          case Some(valueVal) =>
            val patternVal = Constants.mainPattern.findFirstIn(value.toString)
            val readAndSplit = value.toString.split(" ")
            val timeIntervalVal = readAndSplit(0)
            val messageLevel = readAndSplit(2)
            if (messageLevel == "ERROR") {
              val mapkey = Constants.generateTimeInterval(timeIntervalVal.split(":")) + " " + messageLevel
              word.set(mapkey)
              output.collect(word, one)
            }
          case None => None
      } catch {
        case e: Exception => logger.trace("The Exception is" + e.toString)
      }
      logger.info("*******************Exiting ErrorCounterMapper-Map****************")

  class ErrorCounterReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[ErrorCounterReducer])
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterReduce-Reducer****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting ErrorCounterReducer-Reduce****************")

  /*http://codingjunkie.net/secondary-sort*/
  class SortCountMapper extends MapReduceBase with Mapper[LongWritable, Text, IntWritable,Text]:
    override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      logger.info("*******************Entering SortCountMapper-Map****************")
      logger.info("**********CurrVal******" + value.toString)
      val readAndSplit = value.toString.split("\t")
      val timestamp = new Text(readAndSplit(0))
      val countVal = new IntWritable(readAndSplit(1).toInt * -1)
      output.collect(countVal, timestamp)
      logger.info("*******************Exiting SortCountMapper-Map****************")

  class SortCountReducer extends MapReduceBase with Reducer[IntWritable,Text , Text, IntWritable]:
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering SortCountReducer-Reduce****************")
      val keyVal = new IntWritable(key.get() * -1)
      val wordReduce = new Text()
      values.asScala.foreach(value => {
        wordReduce.set(value)
        output.collect(wordReduce, keyVal)
      })
      logger.info("*******************Exiting SortCountReducer-Reduce****************")

}
