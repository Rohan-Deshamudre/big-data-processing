package intro

/**
  * This part has some exercises for you to practice with the recursive lists and functions.
  * For the exercises in this part you are _not_ allowed to use library functions,
  * you should implement everything yourself.
  * Use recursion to process lists, iteration is not allowed.
  *
  * This part is worth 5 points.
  */
object Practice {

    /** Q5 (2p)
      * Implement the function that returns the first n elements from the list.
      *
      * @param xs list to take items from.
      * @param n amount of items to take.
      * @return the first n items of xs.
      */
    def firstN(xs: List[Int], n: Int): List[Int] = {
        xs match{
            case i :: tail if n>0 => i :: firstN(tail,n-1)
            case _ => Nil
        }
    }


    /** Q6 (3p)
      * Implement the function that returns the maximum value in the list.
      *
      * @param xs list to process.
      * @return the maximum value in the list.
      */
    def maxValue(xs: List[Int]): Int = {
        def maxValue(s: List[Int], max: Int): Int = {
            s match{
                case i :: tail => maxValue(tail, if(i>max) i else max)
                case Nil => max
            }
        }
        maxValue(xs,-2147483648)
    }
}
