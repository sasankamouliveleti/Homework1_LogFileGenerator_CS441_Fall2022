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
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.analysis.function.Max

object MRJob4 {
  val logger : Logger = CreateLogger(classOf[MaxLengthMapper])
  class MaxLengthMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val patternToSearch = config.getString("patternToSearch").r
      logger.info("The search pattern is:" + patternToSearch)
      val readAndSplit = value.toString.split(" ")
      val messageLevel = readAndSplit(2)
      word.set(messageLevel)
      if(messageLevel.equals("ERROR") || messageLevel.equals("DEBUG")){
        logger.info("message Level " + messageLevel + " The pattern from logger Val" + readAndSplit(5))
        if (patternToSearch.matches(readAndSplit(5))) {
          logger.info("The pattern from log" + readAndSplit(5))
          output.collect(word, new IntWritable(readAndSplit(5).length))
        } else {
          logger.info("Came into else case")
        }
      } else if(messageLevel.equals("INFO") || messageLevel.equals("WARN")){
        logger.info("message Level " + messageLevel + " The pattern from logger Val" + readAndSplit(6))
        if (patternToSearch.matches(readAndSplit(6))) {
          logger.info("The pattern from log" + readAndSplit(6))
          output.collect(word, new IntWritable(readAndSplit(6).length))
        } else {
          logger.info("Came into else case")
        }
      } else{
        logger.info("Message level match not found")
      }



  class MaxLengthReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val maxValue = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get().max(valueTwo.get())))
      output.collect(key, new IntWritable(maxValue.get()))


  def main(args: Array[String]): Unit =
    logger.info("*******************Entering MRJob1Main****************")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("TimeType")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

    conf.setMapperClass(classOf[MaxLengthMapper])

    conf.setCombinerClass(classOf[MaxLengthReducer])
    conf.setReducerClass(classOf[MaxLengthReducer])

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
    logger.info("*******************Exiting MRJob1Main****************")
}
