
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File, Stats}
import util.{CommitGeoParser, CommitParser}

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    dummy_question(commitStream).print()
    //question_two(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[(String,String, Int)] = {
    val filter = input.flatMap(i=>i.files.filter(i=>(i.filename.get.endsWith(".js")||i.filename.get.endsWith(".py"))&& i.filename.nonEmpty)
      .map(i=>(i.filename.get.substring(i.filename.get.lastIndexOf(".")+1,i.filename.get.length()),i.status.get)))
      .filter(i=>i._1.nonEmpty && i._2.nonEmpty)
      .map(i=>(i._1,i._2,1)).keyBy(i=>i._2).reduce((a,b) =>(a._1,b._2,b._3+a._3))
    filter
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    val nonEmpty = input.filter(i=> i.sha.nonEmpty && i.stats.nonEmpty)
    val res = nonEmpty.filter(i=> i.stats.get.additions>=20).map(i=> i.sha)
    res
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    val nonEmpty = input.filter(i=>i.files.nonEmpty)
    val res = nonEmpty.flatMap(i=>i.files.filter(i=>i.filename.nonEmpty && i.deletions>30).map(_.filename))
    res.map(i=>i.get)
  }


  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val fileFilter = input.flatMap(i=>i.files.filter(i=>(i.filename.get.endsWith(".java")||i.filename.get.endsWith(".scala"))&& i.filename.nonEmpty)
      .map(i=>i.filename.get.substring(i.filename.get.lastIndexOf(".")+1,i.filename.get.length()))).filter(i=>i.nonEmpty).map(i=>(i,1))

    fileFilter.keyBy(i=>i._1).reduce((a,b)=>(a._1,b._2+a._2))
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    val filter = input.flatMap(i=>i.files.filter(i=>(i.filename.get.endsWith(".js")||i.filename.get.endsWith(".py"))&& i.filename.nonEmpty && i.status.nonEmpty)
      .map(i=>(i.filename.get.substring(i.filename.get.lastIndexOf(".")+1,i.filename.get.length()),i.status.get)))
      .filter(i=>i._1.nonEmpty && i._2.nonEmpty)
      .map(i=>(i._1,i._2,1)).keyBy(i=>i._2).reduce((a,b) =>(a._1,b._2,b._3+a._3))
    filter
  }



  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val sdf = new SimpleDateFormat("dd-MM-yyyy")
    val time = input.assignAscendingTimestamps(i=>i.commit.committer.date.getTime)
    val sup = time.map(i=>(i.commit.committer.date,1))
    val bruh = sup.timeWindowAll(Time.hours(24)).reduce((a,b)=>(a._1,a._2+b._2))

    //reduce((a,b)=>(a._1,a._2+b._2))
    bruh.map(i=>(sdf.format(i._1),i._2))
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val time = input.assignAscendingTimestamps(i=>i.commit.committer.date.getTime)
    val size = time.map(i=>i.stats.getOrElse(Stats(0,0,0)).total).map(i=>if (0<=i && i<=20) ("small",1) else ("large",1))
    val res = size.keyBy(0).timeWindow(Time.hours(48), Time.hours(12)).reduce((a,b)=>(a._1,a._2+b._2))
    res
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    val dateformat = new SimpleDateFormat("dd-MM-yyyy")
    val date = commitStream.assignAscendingTimestamps(i=>i.commit.committer.date.getTime)
    val commits = date.map(i=>(i.url.split("\\/")(4).concat("/").concat(i.url.split("\\/")(5)),
      dateformat.format(i.commit.committer.date), (i.commit.committer.name,1), i.stats.get.total,1))
    val perDay = commits.keyBy(i=>i._1).timeWindow(Time.days(1))
    val res = perDay.process(new ProcessWindowFunction[(String,String,(String,Int),Int,Int), CommitSummary, String,TimeWindow] {
      override def process(key: String,
                           context: Context,
                           elements: Iterable[(String, String, (String, Int), Int, Int)],
                           out: Collector[CommitSummary]): Unit = {
        val datefomat = new SimpleDateFormat("dd-MM-yyyy")
        val committers = elements.map(_._3).groupBy(i=>i._1).mapValues(_.map(_._2).size).toList
        val size = committers.maxBy(i=>i._2)
        val fin = committers.filter(i=>i._2 == size._2).sortBy(i=>i._1).map(_._1).toString().drop(5).dropRight(1)

        out.collect(new CommitSummary(key, datefomat.format(context.window.getStart), elements.map(_._5).sum, committers.size, elements.map(_._4).sum,fin))
      }
    }).filter(i=> i.amountOfCommitters <=2 && i.amountOfCommits >20)
    res
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(commitStream: DataStream[Commit], geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {
      commitStream.assignAscendingTimestamps(i=>i.commit.committer.date.getTime)
        .map(i=>(i.sha, i.files.filter(i=>i.filename.nonEmpty && i.filename.get.toString.split("\\.").last.equals("java"))
          .map(i=>i.changes).sum, i.commit.committer.date)).keyBy(0)
        .intervalJoin(geoStream.assignAscendingTimestamps(i=>i.createdAt.getTime).map(i=>(i.sha,i.continent)).keyBy(0))
      .between(Time.hours(-1),Time.minutes(30))
      .process(new ProcessJoinFunction[(String,Int,Date),(String,String),(Date,String,Int)] {
        override def processElement(in1: (String, Int, Date),
                                    in2: (String, String),
                                    context: ProcessJoinFunction[(String, Int, Date), (String, String), (Date, String, Int)]#Context,
                                    collector: Collector[(Date, String, Int)]): Unit = {
          collector.collect((in1._3, in2._2, in1._2))}})
        .assignAscendingTimestamps(i=>i._1.getTime).keyBy(1).timeWindow(Time.days(7)).sum(2)
        .map(i=>(i._2,i._3)).filter(_._2>0)
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = ???

//
//    def repository(repo:String): String = {
//      repo.split("/")(4)+"/"+repo.split("/")(5)
//    }
//
//    def one (str: String, list: List[(String,String)]):List[(String,String,String)] = {
//      list match{
//        case Nil => Nil
//        case x :: tail => (str, x._1, x._2) :: one(str,tail)
//      }
//    }
//
//    val pattern = Pattern.begin[(String,String,String)]("first").where(_._3.equals("added"))
//      .followedBy("end").where(_._3.equals("removed")).within(Time.days(1))
//
//    inputStream.assignAscendingTimestamps(i=>i.commit.committer.date.getTime)
//      .map(i=>(repository(i.url), i.files.filter(i=>i.status.nonEmpty && i.filename.nonEmpty).map(i=>(i.filename.get,i.status.get, 1 ))))
//


}
