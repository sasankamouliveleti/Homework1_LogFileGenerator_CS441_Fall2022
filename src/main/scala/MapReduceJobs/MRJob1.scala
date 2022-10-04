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
    private val config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeMapper****************")
      val readAndSplit = value.toString.split(" ")
      val logTimeStamp = readAndSplit(0)
      val messageLevel = readAndSplit(2)
      val timeSplit = logTimeStamp.split(":")
      logger.info("The time split is" + timeSplit(0) + timeSplit(1) + timeSplit(1))
      val mapkey = Constants.generateTimeInterval(timeSplit) + " " + messageLevel
      word.set(mapkey)
      output.collect(word, one)
      logger.info("*******************Exiting TimeTypeMapper****************")

  class TimeTypeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[TimeTypeReducer])
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeReducer****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting TimeTypeReducer****************")

  def main(args: Array[String]):Unit =
    logger.info("*******************Entering MRJob1Main****************")
    val conf : JobConf = new JobConf(this.getClass)
    conf.setJobName(Constants.MRJob1)

    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

//    conf.set("mapred.textoutputformat.separator", ",")
    conf.setMapperClass(classOf[TimeTypeMapper])

    conf.setCombinerClass(classOf[TimeTypeReducer])
    conf.setReducerClass(classOf[TimeTypeReducer])

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
    logger.info("*******************Exiting MRJob1Main****************")

}
