// Import spark packages
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

// Import scala packages
import shapeless.syntax.std.tuple._

// Import java packages
import java.io._
import java.time._
import java.time.format._
import java.time.temporal._

object Milestone1
{

    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")


    //----------------------------------------------------- Write Functions --------------------------------------------------------------------
    /**
      * Create a new blank file and write to it
      *
      * @param filename
      * @param lines
      */
    def writeFile(filename: String, lines: Array[(Int, String)]): Unit =
    {
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        for (line <- lines) {
            bw.write(line._2 + "\n" + "\n")
        }
        bw.close()
    }

    /**
      * Append to existing file 
      *
      * @param filename
      * @param line
      */
    def appendtoFile(filename: String, line: String): Unit =
    {
        val fw = new FileWriter(filename, true)
        try 
        {
            fw.write(line)
        }
        finally fw.close() 
    }

    /**
      * Helper to write elements of the array for question 3 and first write the start phrase
      *
      * @param filename
      * @param array
      * @param start
      */
    def writeSingleArrayHelper(filename: String, array: Array[String], start: String): Unit =
    {
        appendtoFile(filename, start)
        for(host <- array)
            appendtoFile(filename, ", " + host)
    }

    //----------------------------------------------------- Reduce functions--------------------------------------------------------------------

    /**
      * function that truncates composite keys to remove attempts from the keys after merging
      *
      * @param key
      * @return
      */
    def truncateKey(key: String): String = return key.substring(0, key.lastIndexOf('-'))
    /**
      * function that is used to concatenate after joining different rdds
      *
      * @param value
      * @return
      */
    def concatenateValues(value: (String, String)): String = value._1 + "\n" + value._2

    //----------------------------------------------------- Filter functions--------------------------------------------------------------------
    /** Checks if a line starts with a certain expression to create sub rdds
      * 
      *
      * @param line
      * @return
      */
    def appFilter(line: String): Boolean = line.startsWith("ApplicationId")
    def userFilter(line: String): Boolean = line.startsWith("User")
    def attemptFilter(line: String): Boolean = line.startsWith("AttemptNumber")
    def endFilter(line: String): Boolean = line.startsWith("EndTime")
    def containerFilter(line: String): Boolean = line.startsWith("(")


    //----------------------------------------------------- Getter functions--------------------------------------------------------------------
    /**
      * Extractor functions that get information from the final RDD containing the desired output format to answer the required questions 
      *
      * @param app
      * @return
      */
    def getUser(app: String): String = 
    {
        val userRegex = "User[ \t]+:[ \t]+([a-zA-z]+)".r
        val userLine = userRegex.findFirstIn(app).get
        val userRegex(user) = userLine
        return user
    }
    /**
      * 
      *
      * @param app
      * @return #unsuccessful attempts for each app
      */
    def getUnSuccessfulAttempts(app: String): Int =
    {
        val unSuccessfulRegex = "KILLED|FAILED".r
        return unSuccessfulRegex.findAllIn(app).length
    }

    /**
      * 
      *
      * @param app
      * @return start date of first attempt of the app
      */
    def getFirstDate(app: String): String =
    {
        val dateRegex = "([0-9]{4}-[0-9]{2}-[0-9]{2})".r
        return dateRegex.findFirstIn(app).get
    }

    /**
      * Input is 1 attempt not 1 application
      *
      * @param app
      * @return duration from start of first attempt to end of last attempt of an app
      */
    def getDateTimePeriod(app: String): Long =
    {
        val dateRegex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
        val timeRegex = "([0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3})"
        val startDateTimeString = (dateRegex + " " + timeRegex).r.findFirstIn(app).get
        val endDateTimeString = (dateRegex + " " + timeRegex).r.findAllIn(app).toList.last

        val startDate = LocalDateTime.parse(startDateTimeString, dateTimeFormatter)
        val endDate = LocalDateTime.parse(endDateTimeString, dateTimeFormatter)

        return startDate.until(endDate, ChronoUnit.MILLIS)

    }

    /**
      * Getter function however its inputs are from an intermediary RDD not the final one where each element is in the form (<app_id>, (int, host))
      *
      * @param containerEntry
      * @return host
      */
    def getHost(containerEntry: String): String =
    {
        val hostRegex = """,(.*)\)""".r
        val hostRegex(host) = hostRegex.findFirstIn(containerEntry).get
        return host
    }

    /**
      * takes an rdd of the lines of the log file, parses it to produce desired output and answer the questions
      * has 8 return values the first is an RDD of the apps after parsing and the rest represend the answers to the questions in order
      * @param filteredRDD
      * @return parsed_log_file + answers
      */
    def parseLog(filteredRDD: RDD[String]): (RDD[(Int, String)], (String, Int), (String, Int), String, Long, Long, Array[String], (String, Int)) =
    {

        // Parse each line to desired format if it has relevant information 
        val mappedRDD = filteredRDD.map(line => transformPatterns(line))

        // Seperate each type of information to its own RDD
        val applicationRDD = mappedRDD.filter(entry => appFilter(entry._2))
        val userRDD = mappedRDD.filter(entry => userFilter(entry._2))
        val attemptRDD = mappedRDD.filter(entry => attemptFilter(entry._2))
        val endRDD = mappedRDD.filter(entry => endFilter(entry._2))
        val containerRDD = mappedRDD.filter(entry => containerFilter(entry._2))

        // Concatenate containers from the same attempt of a certain app
        val reducedContainerRDD = containerRDD.reduceByKey(_ + ", " + _).mapValues(entry => "Containers".padTo(14, " ").mkString("") + ": " + entry)



        // Join RDDs to a make a new RDD each one contains a full information about a certain attempt
        val fullAttemptRDD = attemptRDD.join(
            endRDD.join(reducedContainerRDD).mapValues(concatenateValues)          
        ).mapValues(concatenateValues).sortByKey().map(entry => (truncateKey(entry._1), entry._2))

        // Create new RDD that contains the number of attempts of each app
        val numAttempsRDD = fullAttemptRDD.groupByKey().mapValues(value => "NumAttempts".padTo(14, " ").mkString("") + ": " + value.toList.length)

        // Join the remaining RDDs to form the finalRDD that has the desired output format of each app as an element
        val finalRDD = applicationRDD.join(
            userRDD.join(
                numAttempsRDD.join(
                    fullAttemptRDD.reduceByKey(_ + "\n" + _)
                ).mapValues(concatenateValues)
            ).mapValues(concatenateValues)                            
        ).mapValues(concatenateValues).map(entry => (entry._1.toInt, entry._2)).sortByKey()

        // Each line contain the required processing to answer one of the question
        val topUser = finalRDD.map(entry => (getUser(entry._2), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(1)
        val topUnSucc = finalRDD.map(entry => (getUser(entry._2), getUnSuccessfulAttempts(entry._2))).reduceByKey(_ + _).sortBy(_._2, false).take(1)
        val dates = finalRDD.map(entry => (getFirstDate(entry._2), 1)).reduceByKey(_ + _).map(entry => entry._1 + ": " + entry._2).reduce(_ + ", " + _)
        val meanDurationApp = math.round(finalRDD.map(entry => getDateTimePeriod(entry._2)).mean())
        val successAttemptMeanDuration = math.round(fullAttemptRDD.filter(entry => entry._2 contains "FINISHING").map(entry => getDateTimePeriod(entry._2)).mean())
        
        // create RDD[(app, host)] to answer the last two questions
        // The filter on the first line eliminates information from previous epochs
        val containerHostsAppRDD = containerRDD.map(entry => (truncateKey(entry._1), getHost(entry._2))).filter(entry => !(entry._1 == "1" && (entry._2 == "iccluster055.iccluster.epfl.ch" || entry._2 == "iccluster057.iccluster.epfl.ch")))
        val hosts = containerHostsAppRDD.map(entry => entry._2).distinct().sortBy(host => host).collect()
        val mostActiveHost = containerHostsAppRDD.distinct().map(host => (host._2, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(1)

        return (finalRDD, topUser(0), topUnSucc(0), dates, meanDurationApp, successAttemptMeanDuration, hosts, mostActiveHost(0))
    }

    /**
      * function that uses regex to check if a line of the original log file has relevant information or not if yes it parses and produce an output
      * the output format is a tuple (String, String) the first element is a composite key representing the app and the attempt (if attempt specific info)
      * Some outputs have two lines of information when extraction of multiple requirements is possiible to preserve processing
      * @param line
      * @return (key<app-attempt>, proccessedLine)
      */
    def transformPatterns(line: String): (String, String) =
    {
        // Get lines corresponding to application IDs
        val applicationIDRegex = "'(application_[0-9]+_([0-9]+))' is submitted".r
        val applicationID = applicationIDRegex.findFirstIn(line)
        if (applicationID != None)
        {
            val applicationIDRegex(id, cleanedID) = applicationID.get
            return (cleanedID.toInt.toString(), "ApplicationId".padTo(14, " ").mkString("") + ": ".concat(id))
        } 

        // Get lines corresponding to usernames
        val userRegex ="by user .*".r
        val user = userRegex.findFirstIn(line)
        if(user != None)
        {
            val id = "id [0-9]+".r.findFirstIn(line).get.substring(3)
            return (id, "User".padTo(14, " ").mkString("") + ": ".concat(user.get.substring(8)))
        }

        // Initialize date and time Regex
        val attemptRegex = "appattempt_[0-9]+_([0-9]+)_([0-9]+)".r
        val dateRegex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
        val timeRegex = "([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3})"
        val dateTimeRegex = (dateRegex + " " + timeRegex).r

        //Get Start Time
        if(line contains "to LAUNCHED")
        {
            val attempt = attemptRegex.findFirstIn(line)
            val attemptRegex(app, attemptNo) = attempt.get
            val attemptCleaned = attemptNo.toInt.toString()
            val idCleaned = app.toInt.toString()
            return (idCleaned + "-" + attemptCleaned, "AttemptNumber".padTo(14, " ").mkString("") + ": ".concat(attemptCleaned)
             + "\n" + "StartTime".padTo(14, " ").mkString("") + ": ".concat(dateTimeRegex.findFirstIn(line).get))
        }        

        // Get End Time
        val endRegex = "to (FINISHING|KILLED|FAILED) on event = ATTEMPT_UPDATE_SAVED".r
        val end = endRegex.findFirstIn(line)
        if(end != None)
        {
            val appAttemptRegex = "appattempt_[0-9]+_([0-9]+)_([0-9]+)".r
            val attempt = appAttemptRegex.findFirstIn(line).get
            val appAttemptRegex(id, attemptNo) = attempt
            val attemptCleaned = attemptNo.toInt.toString()
            val idCleaned = id.toInt.toString()
            val endRegex(endState) = end.get
            return (idCleaned + "-" + attemptCleaned, "EndTime".padTo(14, " ").mkString("") + ": ".concat(dateTimeRegex.findFirstIn(line).get)
             + "\n" + "FinalStatus".padTo(14, " ").mkString("") + ": ".concat(endState))
        }

        // Get containers
        if(line contains "Assigned container container")
        {
            val containerRegex = "container_e[0-9]+_[0-9]+_([0-9]+)_([0-9]+)_([0-9]+)".r
            val container = containerRegex.findFirstIn(line).get
            val containerRegex(app, attemptNo, containerNo) = container
            val attemptCleaned = attemptNo.toInt.toString()
            val idCleaned = app.toInt.toString()
            val containerCleaned = containerNo.toInt.toString()
            val hostRegex = "iccluster[0-9]+.iccluster.epfl.ch".r
            val host = hostRegex.findFirstIn(line).get

            return (idCleaned + "-" + attemptCleaned, "("+ containerCleaned + "," + host + ")")
        }       
        

        return ("None", "None")
    }

    /**
      * function to filter RDD of the output to apps between 121 and 130
      *
      * @param allApps
      */
    def filterApps(allApps: RDD[(Int, String)]): RDD[(Int, String)] = return allApps.filter(entry => entry._1 >= 121 && entry._1 <= 130)

    def main(args: Array[String]): Unit =
    {
        // Get or Create Spark context
        val conf = new SparkConf().setAppName("Milestone1").setMaster("local")
        val sc = SparkContext.getOrCreate(conf)

        // Read input file
        // val lines = sc.textFile("/home/nabegh/Workspace/systems_for_data/project/milestone_1/src/resources/hadoop-yarn-resourcemanager-iccluster040.log")
        val lines = sc.textFile("hdfs://iccluster040.iccluster.epfl.ch:8020/user/msaid/systems_for_data/project/milestone_1/hadoop-yarn-resourcemanager-iccluster040.log")

        // Input file to log parser to extract relevant information
        val answersRDDs = parseLog(lines)

        // Filter output 
        val filteredRDD = filterApps(answersRDDs._1)
        val outputPath = "answers.txt"
        writeFile(outputPath, filteredRDD.collect())
        appendtoFile(outputPath, "\n1. " + answersRDDs._2._1 + ", " + answersRDDs._2._2)
        appendtoFile(outputPath, "\n2. " + answersRDDs._3._1 + ", " + answersRDDs._3._2)
        appendtoFile(outputPath, "\n3. " + answersRDDs._4)
        appendtoFile(outputPath, "\n4. " + answersRDDs._5)
        appendtoFile(outputPath, "\n5. " + answersRDDs._6)
        writeSingleArrayHelper(outputPath, answersRDDs._7, "\n6. " + answersRDDs._7.length)
        appendtoFile(outputPath, "\n7. " + answersRDDs._8._1 + ", " + answersRDDs._8._2)

    }
}