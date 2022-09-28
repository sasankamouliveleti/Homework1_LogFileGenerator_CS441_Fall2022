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

object MRJob3 {

  class MessageLevelMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val readAndSplit = value.toString.split(" ")
      val messageLevel = readAndSplit(2)
      word.set(messageLevel)
      output.collect(word, one)

  class MessageLevelReducer extends  MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  def main(args: Array[String]): Unit =

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MessageLevelCounter")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

    conf.setMapperClass(classOf[MessageLevelMapper])

    conf.setCombinerClass(classOf[MessageLevelReducer])
    conf.setReducerClass(classOf[MessageLevelReducer])

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)

}
