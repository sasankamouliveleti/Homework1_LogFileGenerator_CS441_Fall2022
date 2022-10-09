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

object MRJob4 {
  val logger : Logger = CreateLogger(classOf[MaxLengthMapper]) /* Defining the logger of type MaxLengthMapper*/
  
  /* The Goal of this mapper is take in a user defined regex and find all the log content which matches the regex and create a context with key value pair of 
  type Text and Text where in the key is message level and the value is matched substring from the log content*/
  class MaxLengthMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private final val one = new IntWritable(1) /* Defining the value to be 1*/
    private val word = new Text()
    
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      logger.info("*******************Entering MaxLengthMapper-Map****************")
      try {
        logger.info("The search pattern is:" + Constants.mainPattern)
        val readAndSplit = value.toString.split(" ") /* Split the input line with space*/
        val messageLevel = readAndSplit(2) /* The third element would be the message level*/
        /* Conditions to check if the message levels are ERROR or DEBUG as the log content lies at different splits for each type*/
        if (messageLevel.equals(Constants.messageLevelError) || messageLevel.equals(Constants.messageLevelDebug)) {
          val matchedRegexStr = Constants.mainPattern.findAllMatchIn(readAndSplit(5)) /* Find all matches w.r.t to the user defined regex*/
          /* Iterate through the matched substrings and create a context of message level as key and matched substring as value*/
          matchedRegexStr.foreach(eachItem =>{
            logger.info("message Level " + messageLevel + " The pattern from logger eachItem Val" + eachItem)
            word.set(messageLevel)
            output.collect(word, new Text(eachItem.toString()))
          })
        }
        /* Conditions to check if the message levels are INFO or WARN as the log content lies at different splits for each type*/
        else if (messageLevel.equals(Constants.messageLeveInfo) || messageLevel.equals(Constants.messageLevelWarn)) {
          val matchedRegexStr = Constants.mainPattern.findAllMatchIn(readAndSplit(6)) /* Find all matches w.r.t to the user defined regex*/
          /* Iterate through the matched substrings and create a context of message level as key and matched substring as value*/
          matchedRegexStr.foreach(eachItem => {
            logger.info("message Level " + messageLevel + " The pattern from logger eachItem Val" + eachItem)
            word.set(messageLevel)
            output.collect(word, new Text(eachItem.toString()))
          })
        } else {
          logger.info("Message level match not found")
        }
      } catch {
        case e: Exception => logger.debug("The Exception Occurred is" + e.toString)
      }
      logger.info("*******************Exiting MaxLengthMapper-Map****************")
  
  /* The Goal of this reducer is take the key values from mapper of type message level : matched regex substring and reduce it find the
   longest substring in each message level which matches the regex. The output of the reducer would be 
   message level, longest substring match, length of the substring*/
  class MaxLengthReducer extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("*******************Entering MaxLengthReducer-Reduce****************")
      /* Getting all the values found of specific message level and map them to a tuple of its value and length*/
      val valuesFound = values.asScala.map(value =>{
        (value.toString,value.toString.length)
      }).toList
      /*Reference - https://stackoverflow.com/questions/2627919/scala-how-can-i-sort-an-array-of-tuples-by-their-second-element*/
      /* Sort the valuesFound list based on the second element of tuple inorder find the longest length*/
      val sortedValues = Constants.sortBasedOnSecondVal(valuesFound)
      /* Now write the result into context where the key would be the messagelevel,largest substring match and the key would be length*/
      output.collect(new Text(key.toString + ","+ sortedValues.last(0)), new IntWritable(sortedValues.last(1)))
      logger.info("*******************Exiting MaxLengthReducer-Reduce****************")
}
