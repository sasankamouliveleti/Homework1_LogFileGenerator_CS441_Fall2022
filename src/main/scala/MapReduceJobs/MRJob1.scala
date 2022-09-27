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


object MRJob1 {
  val logger : Logger = CreateLogger(classOf[TimeTypeMapper])
  class TimeTypeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeMapper Map****************")
      val readAndSplit = value.toString.split(" ")
      val timeIntervalVal = readAndSplit(0)
      val messageLevel = readAndSplit(2)
      val mapKey = timeIntervalVal.slice(0,8) +"_"+ messageLevel
      word.set(mapKey)
      output.collect(word, one)
      logger.info("*******************Exiting TimeTypeMapper Map****************")

  class TimeTypeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering TimeTypeMapper Reduce****************")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting TimeTypeMapper Reduce****************")

  def main(args: Array[String]):Unit =
    logger.info("*******************Entering MRJob1Main****************")
    val conf : JobConf = new JobConf(this.getClass)
    conf.setJobName("TimeType")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[TimeTypeMapper])

    conf.setCombinerClass(classOf[TimeTypeReducer])
    conf.setReducerClass(classOf[TimeTypeReducer])

    conf.setInputFormat(classOf[TextInputFormat])

    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
    logger.info("*******************Exiting MRJob1Main****************")

}
