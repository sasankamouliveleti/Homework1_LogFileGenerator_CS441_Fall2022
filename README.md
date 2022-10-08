
<h1>Homework 1 - Map Reduce Jobs</h1>

<h2>Submission by: Sasanka Mouli Subrahmanya Sri Veleti</h2>

<h2>Objective of the Project:</h2>
<p style="font-size: 20px">To generate various statistics related to log files by leveraging Hadoop Map-Reduce Framework, namely the following 4 tasks </p>
<ol style="font-size: 18px">
<li>To show the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types</li>
<li>To compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances</li>
<li>And for each message type you will produce the number of the generated log messages</li>
<li>To produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern</li>
</ol>
<p style="font-size: 20px">The detailed explanation of the environment set up and the logic behind each implementation is below.</p>

<p style="font-size: 20px">And the AWS EMR deployment video is linked <a href="">here.</a></p>

<h3>Environment and Dependencies used to set up this project</h3>
<ul>
    <li>Operating System: Windows 11 Enterprise, Version - 21H2</li>
    <li>IDE: IntelliJ IDEA Ultimate 2022.2.2</li>
    <li>Java Version - 11.0.16</li>
    <li>Scala Version- 2.12, sbt - Version 1.6.2 </li>
    <li>Hadoop 3.3.4</li>
</ul>

<h4>Few things to make sure Hadoop runs properly</h4>
<ul>
<li>First make sure the hdfs format namenode command successfully formats the namenode</li>
<li>Then running start-all.sh command in hadoop/sbin/ starts all 4 daemons properly namely datanode, namenode, resource manager, nodemanager</li>
<li>When executing any command related to hadoop make sure all the daemons are running.</li>
</ul>

<p style="font-size: 16px">I have implemented Map Reduce Jobs on top of <a href="https://github.com/0x1DOCD00D/CS441_Fall2022/tree/main/LogFileGenerator" target="_blank">LogFileGenerator Project</a> provided by Prof. Mark Grechanik. The output of LogFile Generator will be the input of Map Reduce Jobs.</p>

<h3>Steps to follow to make this project run:</h3>
<ol>
<li>Clone this repository.</li>
<li>Modify the time interval and regex pattern defined in the configuration file based on requirement, the default time interval is 1 minute and regex is ".*". Note: The time interval can only be defined in minutes and between 1 - 60 minutes</li>
<li>Compile the project using the following command</li>

```
sbt clean compile
```
<li>Run the tests using following command</li>

```
sbt test
```
<li>Run the project using following command</li>

```
sbt run
```

as there are multiple classed you will be prompted with the class to choose 1.MRMainJob 2.runLogGenerator.
If you choose 1 you also have supply arguments in the following format

```
sbt "run <MRJobNumber> <Input path> <Output path>"
```
<li>Now let us create Fat JAR, for which use the following command</li>

```
sbt clean compile assembly
```
Once the JAR is created, let us run it in Hadoop environment. 

<li>Make sure all the 4 daemons of hadoop are running and run the following command in the path of JAR file</li>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob <MRJobNumber> <Input Log path> <Output Log path> 
```
Here the MRJobNumber can be 
<ul>
<li>1 for Log Message Level Frequency</li>
<li>2 for Descending Order of Error level frequencies in give time interval</li>
<li>3 for Total count of different message levels</li>
<li>4 for Length of Longest matching substring for given regex</li>
</ul>
</ol>

<h2>Detailed Map Reduce Tasks with their Output</h3>
<h3>Task 1 - To show the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types</h3>

<p>To perform this task run the following command</p>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob 1 <Input Log path> <Output Log path>  
```

<p>Implementation of this task</p>
<ol>
<li>Once the user runs the above command the main method determines which functionality to run and starts the corresponding Map Reduce Job. Firstly the program fetches the time interval and regex pattern from the application config file.</li>
<li>The Mapper class here is <b>TimeTypeMapper</b> which extends MapReduceBase.</li>
<li>The Goal of this Mapper Class is to generate key's of type Text which are in the format of (hh:mm hh:mm Message Level)
  and the Values of type Intwritable which here specifies 1 as we are just 
  mapping each (time interval, message level) in each log line which matches the regex to 1. So key, value pair would ((hh:mm hh:mm messageLevel),1)</li>
<li>The Reducer class here is <b>TimeTypeReducer</b></li>
<li>The Goal of this Reducer is to take the TypeTypeReducer mapper key values of format (hh:mm hh:mm message level):[1,1,1,1]
  and reduce them to (hh:mm hh:mm message level):4 by summing the values iterable</li>
<li>The output for this task with an interval of 1 and user regex ".*" is below</li>

```
15:35 15:36 INFO,1
15:35 15:36 WARN,1
15:36 15:37 DEBUG,1
15:36 15:37 INFO,6
15:36 15:37 WARN,3
15:37 15:38 DEBUG,1
15:37 15:38 ERROR,1
15:37 15:38 INFO,4
15:37 15:38 WARN,3
15:38 15:39 DEBUG,1
15:38 15:39 ERROR,2
15:38 15:39 INFO,7
15:38 15:39 WARN,3
15:39 15:40 DEBUG,1
15:39 15:40 INFO,4
15:39 15:40 WARN,5
15:40 15:41 DEBUG,1
15:40 15:41 INFO,8
15:40 15:41 WARN,1
15:41 15:42 INFO,5
15:41 15:42 WARN,3
15:42 15:43 DEBUG,1
15:42 15:43 INFO,9
15:42 15:43 WARN,2
15:43 15:44 INFO,9
15:43 15:44 WARN,2
15:44 15:45 INFO,9
15:44 15:45 WARN,3
15:45 15:46 DEBUG,1
15:45 15:46 INFO,4
15:45 15:46 WARN,2
```
</ol>

<h3>Task 2 - To compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances</h3>

<p>To perform this task run the following command</p>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob 2 <Input Log path> <Output Log path>  
```

<p>Implementation of this task</p>
<ol>
<li>Once the user runs the above command the main method determines which functionality to run and starts the corresponding Map Reduce Job. Firstly the program fetches the time interval and regex pattern from the application config file.</li>
<li>In order to achieve the desired result I have made use of two Map Reduce tasks. The first Map Reduce gives output in ascending order, the second in descending we will see their full details below.</li>
<li>In the first map reduce the mapper is <b>ErrorCounterMapper</b></li>
<li>The Goal of this mapper is to produce key of type Text which is (hh:mm ERROR) and values of type Intwritable 
  essentially 1 --- (hh:mm ERROR):1</li>
<li>In the mapper based on the user defined time intervals and regex the log lines are read and the logs which match the regex are identified and the time start and end for these is calculated and put into a key, the value would be 1.</li>
<li>The first reducer is <b>ErrorCounterReducer</b></li>
<li>The Goal of this reducer is it takes the ErrorCounterMapper map key values (hh:mm hh:mm Error: [1,1,1,1])  
  and reduce them to (hh:mm hh:mm ERROR):4</li>
<li>The second mapper is <b>SortCountMapper</b></li>
<li>We know that the output of a Map Reduce is always ascending order of the key leveraging this idea 
   in the Map we produce the key value as (-4:hh:mm hh:mm ERROR) this gives ascending order output, the key value
   as example -5:hh:mm hh:mm ERROR, -3: hh:mm hh:mm ERROR, 0:hh:mm hh:mm ERROR</li>
<li>The second reducer is <b>SortCountReducer</b></li>
<li>This reducer takes the key as negative value and the value as time interval and message level and swaps the 
  key values by iterating each value list for a specific key thus giving the output as descending order of error counts</li>
<li>The output for this task with an interval of 1 and regex ".*" is below</li>

```
15:38 15:39 ERROR,2
15:37 15:38 ERROR,1  
```
</ol>
<p>Note: While running task2 there is an intermediate output folder named "outputfilename + "_ascsort"" created which is output of first map reduce and this is input for the second map reduce task</p>

<h3>Task 3 - For each message type you will produce the number of the generated log messages</h3>

<p>To perform this task run the following command</p>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob 3 <Input Log path> <Output Log path>  
```

<p>Implementation of this task</p>
<ol>
<li>Once the user runs the above command the main method determines which functionality to run and starts the corresponding Map Reduce Job. Firstly the program fetches the time interval from the application config file.</li>
<li>The mapper class is <b>MessageLevelMapper</b></li>
<li>The Goal of this Mapper Class is to generate Key's of type Text which are the (Message Level)
    and the Values of type Intwritable which here specifies 1 as we are just mapping the message level in each log line which matches the regex to 1</li>
<li>The reducer class is <b>MessageLevelReducer</b></li>
<li>The Goal of this Reducer is to take the MessageLevelMapper mapper key values of format (message level):[1,1,1,1]
    and reduce them to (message level):4</li>
<li>The output for this task with an interval of 1 is below</li>

```
DEBUG,7
ERROR,3
INFO,66
WARN,28  
```
</ol>

<h3>Task 4 - To produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern</h3>

<p>To perform this task run the following command</p>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob 4 <Input Log path> <Output Log path>  
```

<p>Implementation of this task</p>
<ol>
<li>Once the user runs the above command the main method determines which functionality to run and starts the corresponding Map Reduce Job. Firstly the program fetches the time interval and regex pattern from the application config file.</li>
<li>The mapper class is <b>MaxLengthMapper</b></li>
<li>The Goal of this mapper is take in a user defined regex and find all the log content which matches the regex and create a context with key value pair of 
  type Text and Text where in the key is message level and the value is matched substring from the log content</li>
<li>The reducer class is <b>MaxLengthReducer</b></li>
<li>The Goal of this reducer is take the key values from mapper of type message level : matched regex substring and reduce it find the
   longest substring in each message level which matches the regex. The output of the reducer would be 
   message level, longest substring match, length of the substring</li>
<li>The output for this task with an interval of 1 and regex ".*" is below</li>

```
DEBUG,1vGnsU^9ofOEJ!F'YIcjlEcJYlQL+-sO0UHnYGFCbhaTLi8x0oBs$y"'X!,H,60
ERROR,ihu}!A2]*07}|,lc,16
INFO,=y:wo/f;JGDxr3kYD:KMnEa=,kC5hI9oce1bg3ae1ag2J9ncg2N5oW7fq{W:|tm1u)A}GY`q;JGZ0\xG"[,82
WARN,.0>S+>Ua\Zqk:;@m3o}_PyU{,MZ12aXX%5DlWJ<GU5V'J%d\J.0gOP8fGz;X,60
```
</ol>
