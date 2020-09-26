package dataset

import java.util.TimeZone
import java.text.SimpleDateFormat
import dataset.util.Commit.Commit

/**
  * Use your knowledge of functional programming to complete the following functions.
  * You are recommended to use library functions when possible.
  *
  * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
  * When asked for dates, use the `commit.commit.committer.date` field.
  *
  * This part is worth 40 points.
  */
object Dataset {

    /** Q16 (5p)
      * For the commits that are accompanied with stats data, compute the average of their additions.
      * You can assume a positive amount of usable commits is present in the data.
      * @param input the list of commits to process.
      * @return the average amount of additions in the commits that have stats data.
      */
    def avgAdditions(input: List[Commit]): Int = {

        input.map(i => i.stats.get.additions).sum/input.size
    }

    /** Q17 (8p)
      * Find the hour of day (in 24h notation) during which the most javascript (.js) files are changed in commits.
      * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
      * @param input list of commits to process.
      * @return the hour and the amount of files changed during this hour.
      */
    def jsTime(input: List[Commit]): (Int, Int) = {

        val fileFilter = input.map(i=> (i.commit.committer.date.toInstant.toString.substring(11,13), i.files.filter(i=> i.filename.get.matches(".*\\.js$"))))

        val wanted = fileFilter.map(i=>(i._1,i._2.size))
        val filter = wanted.filter(i=> i._2 != 0)

        val result = filter.groupBy(_._1).mapValues(_.map(_._2).sum).maxBy(_._2)

        return (result._1.toInt,result._2)

    }

    /** Q18 (9p)
      * For a given repository, output the name and amount of commits for the person
      * with the most commits to this repository.
      * @param input the list of commits to process.
      * @param repo the repository name to consider.
      * @return the name and amount of commits for the top committer.
      */
    def topCommitter(input: List[Commit], repo: String): (String, Int) = {
        val repoFilter = input.filter(_.url.toString.contains(repo))
        val topCommitter = repoFilter.groupBy(_.commit.committer.name).mapValues(_.size).maxBy(x=>x._2)

        return topCommitter

    }

    /** Q19 (9p)
      * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
      * Leave out all repositories that had no activity this year.
      * @param input the list of commits to process.
      * @return a map that maps the repo name to the amount of commits.
      *
      * Example output:
      * Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
      */
    def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
        val yearFilter = input.filter(i => i.commit.committer.date.getYear==119)
        val newMap = yearFilter.map(_.url).groupBy(i=>i.substring(29,i.indexOf("/", i.indexOf("/",29)+1)))
        return newMap.transform((key,value)=>value.size)
    }

    /** Q20 (9p)
      * Derive the 5 file types that appear most frequent in the commit logs.
      * @param input the list of commits to process.
      * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
      */
    def topFileFormats(input: List[Commit]): List[(String, Int)] = {
        (input.map(i=> i.files.flatMap(_.filename)).flatten.groupBy(i=>i.substring(i.lastIndexOf(".")+1,i.length)).mapValues(_.size).toList.sortBy(-_._2).take(5))
    }
}
