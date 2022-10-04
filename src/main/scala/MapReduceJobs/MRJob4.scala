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
//import org.apache.commons.math3.analysis.function.Max

object MRJob4 {
  val logger : Logger = CreateLogger(classOf[MaxLengthMapper])
  class MaxLengthMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val patternToSearch = config.getString("patternToSearch").r
      logger.info("The search pattern is:" + patternToSearch)
      val mainPattern = config.getString("patternToSearch").r
      val patternVal = mainPattern.findFirstMatchIn(value.toString)
      patternVal match
        case Some(valueVal) =>
          val readAndSplit = value.toString.split(" ")
          val messageLevel = readAndSplit(2)
          if (messageLevel.equals(Constants.messageLevelError) || messageLevel.equals(Constants.messageLevelDebug)) {
            logger.info("message Level " + messageLevel + " The pattern from logger Val" + readAndSplit(5))
            logger.info("The pattern from log" + readAndSplit(5))
            word.set(messageLevel)
            //output.collect(word, new IntWritable(readAndSplit(5).length))
            output.collect(word, new Text(readAndSplit(5)))
          } else if (messageLevel.equals(Constants.messageLeveInfo) || messageLevel.equals(Constants.messageLevelWarn)) {
            logger.info("message Level " + messageLevel + " The pattern from logger Val " + readAndSplit(6))
            logger.info("The pattern from log " + readAndSplit(6))
            word.set(messageLevel)
            //output.collect(word, new IntWritable(readAndSplit(6).length))
            output.collect(word, new Text(readAndSplit(6)))
          } else {
            logger.info("Message level match not found")
          }
        case None => None


  class MaxLengthReducer extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //val maxValue = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get().max(valueTwo.get())))
      val maxValue = values.asScala.reduce((valueOne, valueTwo) => {
        if(valueOne.getLength > valueTwo.getLength){
          valueOne
        }else{
          valueTwo
        }
      })
      //val outputValue = new Text(maxValue.getLength.toString)
      val outputValue = new IntWritable(maxValue.getLength)
      logger.info("The outputValue is " + outputValue)
      output.collect(new Text(key.toString + ","+ maxValue.toString), outputValue)


  def main(args: Array[String]): Unit =
    logger.info("*******************Entering MRJob1Main****************")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(Constants.MRJob4)

    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

    conf.setMapperClass(classOf[MaxLengthMapper])

    //conf.setCombinerClass(classOf[MaxLengthReducer])
    conf.setReducerClass(classOf[MaxLengthReducer])

    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[Text])

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
    logger.info("*******************Exiting MRJob1Main****************")
}
