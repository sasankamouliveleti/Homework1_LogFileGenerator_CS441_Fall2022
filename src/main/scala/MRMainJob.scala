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
  /* Main method for Map - Reduce Jobs */
  /*
    arg(0) -> Type of Job
    arg(1) -> Input Path
    arg(2) -> Output Path
  */
  def main(args: Array[String]): Unit =
    val conf: JobConf = new JobConf(this.getClass) /* Intialising the Job Config*/
    conf.set(Constants.fileSystemType, Constants.fileSystemTypeVal)
//    conf.set(Constants.noOfMappers, Constants.noOfMappersVal)
//    conf.set(Constants.noOfReducers, Constants.noOfReducersVal)

    /* Condition for Job1 where we compute the message level freguency in log files based user defined 
    time interval and regex*/
    if(args(0).toInt == 1) {
      logger.info("*******************Entering MRJob1Main****************")

      conf.setJobName(Constants.MRJob1) /* Setting the Job's name*/
      conf.set("mapreduce.output.textoutputformat.separator", ",") /* To generate comma separated output*/

      /* Defining the Mapper and Reducer Class Job1*/
      conf.setMapperClass(classOf[TimeTypeMapper])
      conf.setReducerClass(classOf[TimeTypeReducer])

      /* Defining the output key class and output values class types*/
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      /*Setting the input and output paths*/
      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))

      /* Running the Job Client*/
      JobClient.runJob(conf)
      logger.info("*******************Exiting MRJob1Main****************")
    } else if(args(0).toInt == 2){
      /* For Job2 we are in need of two Mappers and Reducers so the First Job would be pertaining to
      generating the ERROR level frequency in ascending order and the Second Job for descending order*/
      logger.info("*******************Entering MRJob2Main****************")

      conf.setJobName(Constants.MRJob2) /* Setting the Job's name*/

      /* Defining the Mapper and Reducer Class Job2 - 1*/
      conf.setMapperClass(classOf[ErrorCounterMapper])
      //conf.setCombinerClass(classOf[ErrorCounterReducer])
      conf.setReducerClass(classOf[ErrorCounterReducer])

      conf.setOutputKeyClass(classOf[Text]) /*Setting First Map-Reduce Output Key Class*/
      conf.setOutputValueClass(classOf[IntWritable]) /* Setting First Map-Reduce Output Value Class*/

      /*Setting the input and output paths*/
      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2) + Constants.output0)) /* Output of 1st Job is input for second Job*/

      /* Running the Job Client*/
      JobClient.runJob(conf)

      /********************************************************************/
      /* Second Map Reduce Job for generating the output in descending order*/
      val conf1: JobConf = new JobConf(this.getClass)

      conf1.setJobName(Constants.MRJob2_Final) /* Setting the Job's name*/
      conf1.set(Constants.fileSystemType, Constants.fileSystemTypeVal)
//      conf1.set(Constants.noOfMappers, Constants.noOfMappersVal)
//      conf1.set(Constants.noOfReducers, Constants.noOfReducersVal)
      conf1.set("mapreduce.output.textoutputformat.separator", ",") /* To generate comma separated output*/

      /* Defining the Mapper and Reducer Class Job2 - 1*/
      conf1.setMapperClass(classOf[SortCountMapper])
      conf1.setReducerClass(classOf[SortCountReducer])

      /* Defining the Map output key class and output values class types*/
      conf1.setMapOutputKeyClass(classOf[IntWritable])
      conf1.setMapOutputValueClass(classOf[Text])

      /* Defining the output key class and output values class types*/
      conf1.setOutputKeyClass(classOf[Text])
      conf1.setOutputValueClass(classOf[IntWritable])

      /*Setting the input and output paths*/
      FileInputFormat.setInputPaths(conf1, new Path(args(2) + Constants.output0))
      FileOutputFormat.setOutputPath(conf1, new Path(args(2)))
      
      /* Running the Job Client*/
      JobClient.runJob(conf1)
      logger.info("*******************Exiting MRJob2Main****************")
    } else if(args(0).toInt == 3){
      logger.info("*******************Entering MRJob3Main****************")
      conf.setJobName(Constants.MRJob3) /* Setting the Job's name*/
      conf.set("mapreduce.output.textoutputformat.separator", ",") /* To generate comma separated output*/

      /* Defining the Mapper and Reducer Class Job3*/
      conf.setMapperClass(classOf[MessageLevelMapper])
      //conf.setCombinerClass(classOf[MessageLevelReducer])
      conf.setReducerClass(classOf[MessageLevelReducer])

      /* Defining the output key class and output values class types*/
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      /*Setting the input and output paths*/
      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))

      /* Running the Job Client*/
      JobClient.runJob(conf)

      logger.info("*******************Exiting MRJob3Main****************")
    } else if(args(0).toInt == 4){
      logger.info("*******************Entering MRJob4Main****************")

      conf.setJobName(Constants.MRJob4) /* Setting the Job's name*/
      conf.set("mapreduce.output.textoutputformat.separator", ",")

      /* Defining the Mapper and Reducer Class Job3*/
      conf.setMapperClass(classOf[MaxLengthMapper])
      conf.setReducerClass(classOf[MaxLengthReducer])

      /* Defining the Map output key class and output values class types*/
      conf.setMapOutputKeyClass(classOf[Text])
      conf.setMapOutputValueClass(classOf[Text])

      /* Defining the output key class and output values class types*/
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])

      /*Setting the input and output paths*/
      FileInputFormat.setInputPaths(conf, new Path(args(1)))
      FileOutputFormat.setOutputPath(conf, new Path(args(2)))
      
      /* Running the Job Client*/
      JobClient.runJob(conf)

      logger.info("*******************Exiting MRJob4Main****************")
    }
}
