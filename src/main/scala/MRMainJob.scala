import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import HelperUtils.{Constants, CreateLogger}
import MapReduceJobs.MRJob1.{TimeTypeMapper, TimeTypeReducer, logger}
import MapReduceJobs.MRJob2.{ErrorCounterMapper, ErrorCounterReducer, SortCountMapper, SortCountReducer}
import MapReduceJobs.MRJob3.{MessageLevelMapper, MessageLevelReducer}
import MapReduceJobs.MRJob4.{MaxLengthMapper, MaxLengthReducer}
import com.typesafe.config.ConfigFactory
import org.slf4j.Logger



object MRMainJob {
  def main(args: Array[String]): Unit =
    logger.info(Constants.fileSystemType + "hello")
    val conf: JobConf = new JobConf(this.getClass)
    conf.set(Constants.fileSystemType, Constants.fileSystemTypeVal)
    conf.set(Constants.noOfMappers, Constants.noOfMappersVal)
    conf.set(Constants.noOfReducers, Constants.noOfReducersVal)
    if(args(0).toInt == 1) {

      logger.info("*******************Entering MRJob1Main****************")

      conf.setJobName(Constants.MRJob1)
      conf.set("mapreduce.output.textoutputformat.separator", ",")

      conf.setMapperClass(classOf[TimeTypeMapper])
      conf.setReducerClass(classOf[TimeTypeReducer])

      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))

      JobClient.runJob(conf)
      logger.info("*******************Exiting MRJob1Main****************")
    } else if(args(0).toInt == 2){
      logger.info("*******************Entering MRJob2Main****************")
      conf.setJobName(Constants.MRJob2)
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      conf.setMapperClass(classOf[ErrorCounterMapper])

      //conf.setCombinerClass(classOf[ErrorCounterReducer])
      conf.setReducerClass(classOf[ErrorCounterReducer])

      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))
      JobClient.runJob(conf)

      val conf1: JobConf = new JobConf(this.getClass)

      conf1.setJobName(Constants.MRJob2_Final)
      conf1.set(Constants.fileSystemType, Constants.fileSystemTypeVal)
      conf1.set(Constants.noOfMappers, Constants.noOfMappersVal)
      conf1.set(Constants.noOfReducers, Constants.noOfReducersVal)
      conf1.set("mapreduce.output.textoutputformat.separator", ",")
      conf1.setMapperClass(classOf[SortCountMapper])
      conf1.setReducerClass(classOf[SortCountReducer])

      conf1.setMapOutputKeyClass(classOf[IntWritable])
      conf1.setMapOutputValueClass(classOf[Text])

      conf1.setOutputKeyClass(classOf[Text])
      conf1.setOutputValueClass(classOf[IntWritable])

      FileInputFormat.setInputPaths(conf1, new Path(args(2)))
      FileOutputFormat.setOutputPath(conf1, new Path(args(2) + "_final"))

      JobClient.runJob(conf1)
      logger.info("*******************Exiting MRJob2Main****************")
    } else if(args(0).toInt == 3){
      logger.info("*******************Entering MRJob3Main****************")
      conf.setJobName(Constants.MRJob3)
      conf.set("mapreduce.output.textoutputformat.separator", ",")
      conf.setMapperClass(classOf[MessageLevelMapper])

      //conf.setCombinerClass(classOf[MessageLevelReducer])
      conf.setReducerClass(classOf[MessageLevelReducer])

      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))
      JobClient.runJob(conf)

      logger.info("*******************Exiting MRJob3Main****************")
    } else if(args(0).toInt == 4){
      logger.info("*******************Entering MRJob4Main****************")

      conf.setJobName(Constants.MRJob4)
      conf.set("mapreduce.output.textoutputformat.separator", ",")

      conf.setMapperClass(classOf[MaxLengthMapper])
      conf.setReducerClass(classOf[MaxLengthReducer])

      conf.setMapOutputKeyClass(classOf[Text])
      conf.setMapOutputValueClass(classOf[Text])

      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))
      JobClient.runJob(conf)

      logger.info("*******************Exiting MRJob4Main****************")
    }

}
