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
  val logger : Logger = CreateLogger(classOf[MessageLevelReducer]) /* Defining the logger of MessageLevelMapper Class*/

  /* The Goal of this Mapper Class is to generate Key's of type Text which are the (Message Level)
    and the Values of type Intwritable which here specifies 1 as we are just mapping the message level in each log line which matches the regex to 1*/ 
  class MessageLevelMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1) /* Defining the value to be 1*/
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MessageLevelMapper-Map****************")
      try {
        val readAndSplit = value.toString.split(" ") /* Split the input line with space*/
        val messageLevel = readAndSplit(2) /* The third element would be the message level*/
        word.set(messageLevel) /* Setting the map key to message level, the value would be 1*/
        /* Setting the key and value to the context output*/
        output.collect(word, one)
      } catch {
        case e: Exception => logger.debug("The Exception is" + e.toString)
      }
      logger.info("*******************Exiting MessageLevelMapper-Map****************")

  /* The Goal of this Reducer is to take the MessageLevelMapper mapper key values of format (message level):[1,1,1,1]
    and reduce them to (message level):4*/
  class MessageLevelReducer extends  MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MessageLevelReducer****************")
      /* Reduces by suming all the values in the values iterator*/
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      /* Output the key(message level) and value(sum)* to the context*/
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Entering MessageLevelReducer****************")
}
