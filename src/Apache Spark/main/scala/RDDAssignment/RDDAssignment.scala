package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = {
    commits.count()
  }

  /**
    * We want to know how often programming languages are used in committed files. We require a RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return  RDD containing tuples indicating the programming language (extension) and number of occurrences.
    *
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {
    val fileName = commits.flatMap(i=> i.files.map(i=> i.filename.get))
    val fileExt = fileName.map(i=> {
      if(i.toString.split("[.]").length>1) i.split("[.]").last else "unknown"}).groupBy(_.self)
    val res = fileExt.mapValues(i=> i.size.toLong)
    res
  }

  /**
    * Competitive users on Github might be interested in their ranking in number of commits. We require as return a
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit authors name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing commit author names and total count of commits done by the author, in ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    val authors = commits.map(i=> i.commit.author.name).groupBy(_.self)
    val numComs = authors.mapValues(i=> i.size.toLong).sortBy(_._1).sortBy(i=> -i._2)
    val rank = numComs.zipWithIndex().map(i=> (i._2, i._1._1, i._1._2.toLong))

    rank
  }

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we an RDD that
    * contains the committer name and the total of their commits. As stats are optional, missing Stat cases should be
    * handles as s"Stat(0, 0, 0)". If an User is given that is not in the dataset, then the username should not occur in
    * the return RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    val authors = commits.map( i=> (i.commit.committer.name, i.stats.getOrElse(Stats(0,0,0)))).groupBy(i=>i._1).filter(i=> users.contains(i._1))
    val userStats = authors.mapValues(i=>
      i.aggregate(Stats(0,0,0))((a,b)=>Stats(a.total + b._2.total, a.additions + b._2.additions, a.deletions + b._2.deletions),
        (a,b)=>Stats(a.total, a.additions, a.deletions)))

    userStats
  }


  /**
    * There are different types of people, those who own repositories, and those who make commits. Although Git blame is
    * excellent in finding these types of people, we want to do it in Spark. We require as output an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given RDD. Note that the repository owner is contained within Github urls.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the username that have either only committed to repositories or only own
    *         repositories.
    */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {
    val writes = commits.map(i=> (i.commit.author.name,1))
    val owns = commits.map(i=>(i.url.toString.split("/").toList(4),1))

    owns.fullOuterJoin(writes).filter(i=>i._2._1.nonEmpty || i._2._2.nonEmpty).map(i=>i._1).distinct()
  }

  /**
    * Sometimes developers make mistakes, sometimes they make many. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit. Note that for a commit to be eligible for a 'commit streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    * We require as return a RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest streak of an user and how often said streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuple type containing a commit author username, and a tuple containing the length of the longest
    *         commit streak as well its frequency.
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    val revMes = commits.map(i => (i.commit.message, i.commit.author.name)).filter(i => i._1.startsWith("Revert"))
    val count = revMes.map(i=>(i._1,i._2,i._1.sliding("Revert".length).count(_=="Revert"))).groupBy(i=>i._2)
    val res = count.mapValues(i=> (i.maxBy(i=>i._3), i.count(j=>j._3 ==i.maxBy(j=>j._2)._3)))
    res.map(i=>(i._1,(i._2._1._3,i._2._2)))
  }


  /**
    * We want to know the number of commits that are made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to the repository. Note that from
    * this exercise on, expensive functions like groupBy are no longer allowed to be used. In real life these wide
    * dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDD's.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing a tuple indicating the repository name, the number of commits made to the repository as
    *         well as the unique committer usernames that committed to the repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {
    val tuple = commits.map(i=> (i.url.split("/")(5),(1L,i.commit.committer.name)))
    val reduce = tuple.reduceByKey((a,b) => (a._1 + b._1, a._2 + "/./././." + b._2))
    val res = reduce.map({
      case(x,y) => (x,y._1,y._2.split("/\\./\\./\\./\\.").distinct.toIterable)
    })
    res
  }

  /**
    * Return RDD of tuples containing the repository name and all the files that are contained in that repository.
    * Note that the file names must be unique, so if files occur multiple times (for example due to removal, or new
    * additions), the newest File object must be returned. As the files' filenames are an `Option[String]` discard the
    * files that do not have a filename.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    def helper(help: (Timestamp, List[File], String )): List[(Timestamp, File, String)] = {
      if(help._2.isEmpty){
        List()
      }
      else{
        (help._1, help._2.head, help._3) :: helper((help._1,help._2.tail,help._3))
      }
    }

    val tuple  = commits.map(i=> helper((i.commit.author.date, i.files.filter(_.filename.isDefined), i.url.split("/")(5))))
    val flatTup = tuple.flatMap(i=>i).map(i=> (i._2.filename.get, (i._1, i._3, i._2)))
    val res = flatTup.reduceByKey((a,b) =>
    if(a._1.after(b._1)) a
    else b).map(i=> (i._2._2, Iterable(i._2._3))).reduceByKey((a,b)=> a.toList ::: b.toList).map(i=> (i._1, i._2))
    res
  }


  /**
    * For this assignment you are asked to find all the files of a single repository. This in order to create an
    * overview of each files, do this by creating a tuple containing the file name, all corresponding commit SHA's
    * as well as a Stat object representing the changes made to the file.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = ???

  /**
    * We want to generate an overview of the work done by an user per repository. For this we request an RDD containing a
    * tuple containing the committer username, repository name and a `Stats` object. The Stats object containing the
    * total number of additions, deletions and total contribution.
    * Note that as Stats are optional, therefore a type of Option[Stat] is required.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples of committer names, repository names and and Option[Stat] representing additions and
    *         deletions.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    val tuple = commits.map(i=> (i.commit.committer.name, "repos\\/.*\\/(.*)\\/commits".r.findFirstMatchIn(i.url).get.group(1), i.stats))
    val stats = tuple.map(i=> if (i._3.nonEmpty) (i._1, i._2, i._3.get) else (i._1,i._2,Stats(0,0,0))).keyBy(_._2)
    val agg = stats.aggregateByKey(("","",Stats(0,0,0)))(
      (a,b) => (b._1,b._2,Stats(a._3.total+b._3.total,a._3.additions+b._3.additions,a._3.deletions+b._3.deletions)),
      (a,b) => (b._1,b._2,Stats(a._3.total+b._3.total,a._3.additions+b._3.additions,a._3.deletions+b._3.deletions)))
    val res = agg.map(_._2).map(i=> if(i._3 == Stats(0,0,0)) (i._1,i._2,None) else (i._1,i._2,Some(i._3)))
    res
  }


  /**
    * Hashing function that computes the md5 hash from a String, which in terms returns a Long to act as a hashing
    * function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB from the inputted String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories, use the md5HashString function above to create unique
    * identifiers for the creation of the graph. This exercise is meant as an extra, and is not mandatory to complete.
    * As the real usage Sparks GraphX library is out of the scope of this course, we will not go further into this, but
    * this can be used for algorithms like PageRank, Hubs and Authorities, clique finding, ect.
    *
    * We expect a node for each repository and each committer (based on committer name). We expect an edge from each
    * committer to the repositories that the developer has committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this (complementatry) exercise.
    * Your vertices should contain information about the type of node, a 'developer' or a 'repository' node.
    * Edges should only exist between repositories and committers.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
