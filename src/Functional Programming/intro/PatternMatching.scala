package intro

/**
  * This part gives you some basic information about pattern matching.
  * For the exercises in this part you are _not_ allowed to use library functions,
  * you should implement recursive functions yourself using pattern matching.
  *
  * This part is worth 4 points.
  */
object PatternMatching {

    /**
      * Recursively sums all values in the list.
      *
      * @param xs the list to process.
      * @return the sum of all values in xs.
      *
      */
    def sum(xs: List[Int]): Int = {

        // Using `xs match` we "match" the value of the list.
        // This is called pattern matching.
        xs match {

            // The base case of the recursive definition:
            // the sum of an empty list is 0.
            case Nil => 0

            // If the list has a number,
            // add the value of the head to the sum of the tail
            case i :: tail => i + sum(tail)
        }
    }

    /**
      * These case classes are used for the next function.
      * Note that `OptionalNum` cannot be instantiated:
      * it's either `None`, indicating no value,
      * or `Num(i)`, indicating a value.
      *
      * Scala has similar built-in classes:
      * `Option`, extended by `None()` and `Some(v)`.
      */
    sealed abstract class OptionalNum()
    case class Nothing()    extends OptionalNum
    case class Num(i: Int)  extends OptionalNum

    /**
      * Returns the sum of all defined numbers in a list of optional values.
      * @param xs list of optional numeric values.
      * @return the sum of all defined numbers in `xs`.
      */
    def optionalSum(xs: List[OptionalNum]): Int = xs match {
        // Note that the `xs match` can be placed right after the function definition.

        // Case classes allow for pattern matching. Instead of an if statement to check
        // if the value is a Nothing or a Num, they can be matched in the expression like so:
        case Num(x) :: t => x + optionalSum(t)
        case Nothing() :: t => optionalSum(t)

        // Don't forget the base case!
        case Nil => 0
    }

    /** Q3 (2p)
      * Implement this function that returns the first number in xs that is divisible by n.
      *
      * @param xs the list to check.
      * @param n the number to divide by.
      * @return the first number in xs that is divisible by n, or Nothing if no such number exists.
      *
      * Hint: you can use if statements in pattern matching.
      */
    def firstDivByX(xs: List[Int], n: Int): OptionalNum = {
    xs match{
        case Nil => Nothing()
        case start :: head if start % n == 0 => Num(start)
        case start :: tail if start % n != 0 => firstDivByX(tail, n)
    }

    }

    /** Q4 (2p)
      * Implement this function that returns a list of only the even numbers.
      * All undefined values are left out of the output.
      *
      * @param xs the list to process.
      * @return the list of all even numbers in xs.
      */
    def onlyEvenNumbers(xs: List[OptionalNum]): List[Int] = {
        xs match {
            case Nil => Nil
            case Num(x) :: tail if x%2==0 => x::onlyEvenNumbers(tail)
            case Num(x) :: tail if x%2!=0 => onlyEvenNumbers(tail)
            case Nothing() :: tail => onlyEvenNumbers(tail)
        }
    }
}
