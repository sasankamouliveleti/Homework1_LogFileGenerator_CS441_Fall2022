
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

<li>Make sure all the 4 daemons of hadoop are running and run the following command</li>

```
hadoop jar LogFileMap-Reduce-assembly-0.1.jar MRMainJob <MRJobNumber> <Input Log path> <Output Log path> 
```
Here the MRJobNumber can be 
<ul>
<li>1 - Log Message Level Frequency</li>
<li>2 - Descending Order of Error level frequencies in give time interval</li>
<li>3 - Total count of different message levels</li>
<li>4 - Longest matching substring for given regex</li>
</ul>
</ol>
