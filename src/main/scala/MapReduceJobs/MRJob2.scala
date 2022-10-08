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


object MRJob2 {
  val logger : Logger = CreateLogger(classOf[ErrorCounterMapper]) /* Defining the logger of ErrorCounterMapper*/

  /* The Goal of this mapper is to produce key of type Text which is (hh:mm ERROR) and values of type Intwritable 
  essentially 1 --- (hh:mm ERROR):1*/
  class ErrorCounterMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1) /* Defining the value to be 1*/
    private val word = new Text() /*Defining the word parameter where we will store the key of the mapper*/

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        /* Fetching the user defined search pattern from Constants(which gets it from app.conf) and comparing with the input line*/
        val patternVal = Constants.mainPattern.findFirstIn(value.toString)
        /* for each result case of above statement*/
        patternVal match
          /* Case when there is a match*/
          case Some(valueVal) =>
            val readAndSplit = value.toString.split(" ") /* Split the input line with space*/
            val timeIntervalVal = readAndSplit(0) /* The first element would be the logTimeStamp*/
            val messageLevel = readAndSplit(2) /* The third element would be the message level*/
            if (messageLevel == Constants.messageLevelError) {
              /* Generating the map key for the mapper as hh:mm hh:mm ERROR*/
              val mapkey = Constants.generateTimeInterval(timeIntervalVal.split(":")) + " " + messageLevel
              word.set(mapkey)
              /* Setting the key and value to the context output*/
              output.collect(word, one)
            }
          /* For case when there is no match do nothing */
          case None => None
      } catch {
        case e: Exception => logger.debug("The Exception is" + e.toString)
      }
      logger.info("*******************Exiting ErrorCounterMapper-Map****************")

  /* The Goal of this reducer is it takes the ErrorCounterMapper map key values (hh:mm hh:mm Error: [1,1,1,1])  
  and reduce them to (hh:mm hh:mm ERROR):4*/
  class ErrorCounterReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger : Logger = CreateLogger(classOf[ErrorCounterReducer]) /* Defining the logger of ErrorCounterReducer Class*/
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering ErrorCounterReduce-Reducer****************")
      /* Reduces by suming all the values in the values iterator*/
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      /* Output the key(hh:mm hh:mm ERROR) and value(sum)* to the context*/
      output.collect(key, new IntWritable(sum.get()))
      logger.info("*******************Exiting ErrorCounterReducer-Reduce****************")

  /*Referred http://codingjunkie.net/secondary-sort for sorting in descending order*/
  /* We know that the output of a Map Reduce is always ascending order of the key leveraging this idea 
   in the Map we produce the key value as (-4:hh:mm hh:mm ERROR) this gives ascending order output, the key value
   as an example would be-5:hh:mm hh:mm ERROR, -3: hh:mm hh:mm ERROR, 0:hh:mm hh:mm ERROR
   */
  class SortCountMapper extends MapReduceBase with Mapper[LongWritable, Text, IntWritable,Text]:
    override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      logger.info("*******************Entering SortCountMapper-Map****************")
      logger.info("**********CurrVal******" + value.toString)
      val readAndSplit = value.toString.split(Constants.escapeTab) /* Splitting the input line with tab space*/
      val timestamp = new Text(readAndSplit(0)) /* The first element is timestamp*/
      val countVal = new IntWritable(readAndSplit(1).toInt * -1) /* we multiply the 2nd element with -1 inorder to get a negative count value which aids in sorting in descending order*/
      output.collect(countVal, timestamp) /* write to the context with negative value as key and timestamp and ERROR as value*/
      logger.info("*******************Exiting SortCountMapper-Map****************")

  /* This reducer takes the key as negative value and the value as time interval and message level and swaps the 
  key values by iterating each value list for a specific key thus giving the output as descending order of error counts*/
  class SortCountReducer extends MapReduceBase with Reducer[IntWritable,Text , Text, IntWritable]:
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering SortCountReducer-Reduce****************")
      val keyVal = new IntWritable(key.get() * -1) /* Convert the key to actual count*/
      val wordReduce = new Text()
      val valuesList = values.asScala.toList /* Convert the util.Iterator type toList inorder to loop through the values having the same key*/
      logger.info("The values are" + valuesList)
      valuesList.foreach(value => {
        wordReduce.set(value)
        output.collect(wordReduce, keyVal) /* write to the context by swapping the key and values*/
      })
      logger.info("*******************Exiting SortCountReducer-Reduce****************")

}
