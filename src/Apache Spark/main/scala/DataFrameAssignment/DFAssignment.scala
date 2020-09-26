package DataFrameAssignment

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.spark_project.guava.base.Functions



/**
  * Note read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of commit committers. We require these to be
    * in order according to timestamp.
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: try to work out the individual stages of the exercises, which makes it easier to track bugs, and figure out
    * how Spark Dataframes and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the Dataframes.
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @param authors Sequence of String representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_1(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val one = commits.select("commit.committer.name","sha","commit.committer.date")
    val two = one.filter(i=> authors.contains(i.get(0))).sort("date")
      .withColumnRenamed("name","committer").withColumnRenamed("date","timestamp")
    two
  }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    * The returned DataFrame that is expected is in the following format:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return Dataframe containing 4 columns, Repository name, week number, year and the number fo commits for that
    *         week.
    */
  def assignment_2(commits: DataFrame): DataFrame = {
    val one = commits
      .withColumn("year",year(commits("commit.committer.date")))
      .withColumn("week", weekofyear(commits("commit.committer.date")))
      .withColumn("repository",regexp_replace(commits("url"),".*repos\\/.*\\/(.*)\\/commits.*","$1"))
      .select("repository","week","year").groupBy("repository","week","year")
      .agg(count("*").as("count"))

    one.show()
    one
  }

  /**
    * A developer is interested in the age of commits in seconds, although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore we
    * require you to append the inputted DataFrame with an age column of each commit in `seconds`.
    *
    * Hint: Look into SQL functions in for Spark SQL.
    *
    * Expected Dataframe (column) example that is expected:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return the inputted DataFrame appended with an age column.
    */
  def assignment_3(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
    val ans = commits.withColumn("age",  -to_timestamp(col("commit.committer.date")).cast("long") + snapShotTimestamp.getTime/1000)
    ans.show()
    ans
  }

  /**
    * To perform analysis on commit behavior the intermediate time of commits is needed. We require that the DataFrame
    * that is put in is appended with an extra column that expresses the number of days there are between the current
    * commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit regard the time difference in days should be zero. Make sure to return the
    * commits in chronological order.
    *
    * Hint: Look into Spark sql's Window to have more expressive power in custom aggregations
    *
    * Expected Dataframe example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                   `println(commits.schema)`.
    * @param authorName Name of the author for which the result must be generated.
    * @return DataFrame with column expressing days since last commit.
    */
  def assignment_4(commits: DataFrame, authorName: String): DataFrame = {
    val clap = commits.select("sha","commit.committer.name","commit.committer.date").where(col("name").equalTo(authorName)).sort("date")
    val my_window = Window.partitionBy().orderBy(col("date"))
    val kick = clap.withColumn("prev", lag(col("date"),1).over(my_window))
    val hihat = kick.withColumn("time_diff",when(isnull(datediff(col("date"),col("prev"))),0).otherwise(datediff(col("date"),col("prev"))))
    val snare = hihat.select(col("sha").as("$oid"),col("name"),col("date"),col("time_diff"))
    snare.show()
    snare

  }

  /**
    * To get a bit of insight in the spark SQL, and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits
    * commit date. Sunday would be 1, Monday 2, etc.
    *
    * Expected Dataframe example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 0   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` representing a count of the total number of
    *         commits that that were ever made on that week day.
    */
  def assignment_5(commits: DataFrame): DataFrame = {
    commits.withColumn("day", dayofweek(col("commit.committer.date"))).groupBy("day").count()
  }

  /**
    * Commits can be uploaded on different days, we want to get insight in difference in commit time of the author and
    * the committer. Append the given dataframe with a column expressing the number of seconds in difference between
    * the two events in the commit data.
    *
    * Expected Dataframe (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return original Dataframe appended with a column `commit_time_diff` containing the number of seconds time
    *         difference between authorizing and committing.
    */
  def assignment_6(commits: DataFrame): DataFrame = {
   commits.withColumn("commit_time_diff", -to_timestamp(col("commit.author.date")).cast("long") + to_timestamp(col("commit.committer.date")).cast("long"))
  }

  /**
    * Using Dataframes find all the commit SHA's from which a branch was created, including the number of
    * branches that were made. Only take the SHA's into account if they are also contained in the RDD.
    * Note that the returned Dataframe should not contain any SHA's of which no new branches were made, and should not
    * contain a SHA which is not contained in the given Dataframe.
    *
    * Expected Dataframe example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the SHAs of which a new branch was made.
    */
  def assignment_7(commits: DataFrame): DataFrame = ???

  /**
    * Find of commits from which a fork was created in the given commit DataFrame. We are interested in the name of
    * repositories, the parent and the subsequent fork, including the name of the repository owner. The SHA from which
    * the fork was created (parent_sha) as well as the first SHA that occurs in the forked branch (child_sha).
    *
    * Expected Dataframe example:
    *
    * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
    * |----------------------|---------------------|----------------------|----------------------|
    * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
    * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
    * | ...                  | ...                 | ...                  | ...                  |
    *
    * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
    * help during debugging your solution.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the SHAs of which a new fork was created.
    */
  def assignment_8(commits: DataFrame): DataFrame = {
    val one = commits.select(col("sha"),col("url"), explode(col("parents")).as("parent_sha")).withColumnRenamed("sha", "child_sha")
    val two = one.select("parent_sha.sha","child_sha", "url").withColumnRenamed("sha", "parent_sha").withColumnRenamed("url", "child_repo")
    val three = two.join(commits,col("parent_sha") === commits("sha"))
      .withColumn("repo_name", regexp_replace(col("url"),".*repos\\/(.*)\\/commits.*", "$1"))
      .withColumn("child_repo_name", regexp_replace(col("child_repo"),".*repos\\/(.*)\\/commits.*", "$1"))
    val ans =three.select("repo_name","child_repo_name","parent_sha","child_sha").where(col("repo_name").notEqual(col("child_repo_name")))
    ans
  }
}
