package MapReduceJobs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import HelperUtils.{Constants, CreateLogger}
import com.typesafe.config.ConfigFactory
import org.slf4j.Logger

object MRJob3 {
  val logger : Logger = CreateLogger(classOf[MessageLevelReducer])
  class MessageLevelMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MessageLevelMapper-Map****************")
      try {
        val readAndSplit = value.toString.split(" ")
        val messageLevel = readAndSplit(2)
        word.set(messageLevel)
        output.collect(word, one)
      } catch {
        case e: Exception => logger.trace("The Exception is" + e.toString)
      }
      logger.info("*******************Exiting MessageLevelMapper-Map****************")


  class MessageLevelReducer extends  MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MessageLevelReducer****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Entering MessageLevelReducer****************")
}
