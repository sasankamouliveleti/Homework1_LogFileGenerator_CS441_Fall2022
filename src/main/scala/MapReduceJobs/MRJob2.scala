package MapReduceJobs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import HelperUtils.CreateLogger
import org.slf4j.Logger


object MRJob2 {
  val logger : Logger = CreateLogger(classOf[ErrorCounterMapper])
  class ErrorCounterMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterMapper****************")

      val readAndSplit = value.toString.split(" ")
      val timeIntervalVal = readAndSplit(0)
      val messageLevel = readAndSplit(2)
      if(messageLevel == "ERROR"){
        word.set(timeIntervalVal.slice(0,8))
        output.collect(word, one)
      }
      logger.info("*******************Exiting ErrorCounterMapper****************")

  class ErrorCounterReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[ErrorCounterReducer])
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterReducer****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting ErrorCounterReducer****************")


  class SortCountMapper extends MapReduceBase with Mapper[LongWritable, Text, IntWritable,Text]:
    override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      logger.info("**********CurrVal******" + value.toString)
      val readAndSplit = value.toString.split("\t")
      val timestamp = new Text(readAndSplit(0))
      val countVal = new IntWritable(readAndSplit(1).toInt * -1)
      output.collect(countVal, timestamp)

  class SortCountReducer extends MapReduceBase with Reducer[IntWritable,Text , Text, IntWritable]:
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val keyVal = new IntWritable(key.get() * -1)
      val wordReduce = new Text()
      values.asScala.foreach(value => {
        wordReduce.set(value)
        output.collect(wordReduce, keyVal)
      })


  def main(args: Array[String]): Unit =
    logger.info("*******************Entering MRJob2Main****************")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("ErrorType")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[ErrorCounterMapper])

    conf.setCombinerClass(classOf[ErrorCounterReducer])
    conf.setReducerClass(classOf[ErrorCounterReducer])

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
/////////////////////////////////////////////////////////////////////////////
    val conf1: JobConf = new JobConf(this.getClass)
    conf1.setJobName("ErrorType1")

    conf1.set("fs.defaultFS", "local")
    conf1.set("mapreduce.job.maps", "1")
    conf1.set("mapreduce.job.reduces", "1")

    conf1.setMapperClass(classOf[SortCountMapper])
    conf1.setReducerClass(classOf[SortCountReducer])

    conf1.setMapOutputKeyClass(classOf[IntWritable])
    conf1.setMapOutputValueClass(classOf[Text])

    conf1.setOutputKeyClass(classOf[Text])
    conf1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf1, new Path(args(1)))
    FileOutputFormat.setOutputPath(conf1, new Path(args(1) + "_final"))

    JobClient.runJob(conf1)
    logger.info("*******************Exiting MRJob2Main****************")
}
