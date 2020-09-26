package intro

/**
  * This part teaches you about writing functions in Scala.
  * It is worth 3 points.
  */
object Functions {

    /**
      * An example to show what function definitions look like in Scala.
      *
      * @param i a number
      * @return i * 5 + 2
      */
    def simpleMath(i: Int): Int = {

        // In functional programming variables are not used, only constants.
        // In Scala these are called `val` (for value). They cannot be reassigned.
        val temp = i * 5

        // The `return` keyword is optional:
        // the last value produced is returned automatically
        temp + 2
    }


    /** Q1 (3p)
      * Define the function `fizzBuzz` that operates as follows:
      *
      * @param i an integer number
      * @return
      *           - "Fizz" if the given number is divisible by 3
      *           - "Buzz" if the given number is divisible by 5
      *           - "FizzBuzz" if the given number is divisible by both 3 and 5
      *           - Otherwise return the string representation of the number
      */

    def fizzBuzz(i: Int): String = {
        if(i%3==0 && i%5==0){
            "FizzBuzz"
        }
        else if(i%5==0){
            "Buzz"
        }
        else if(i%3==0){
            "Fizz"
        }
        else{
            i.toString
        }

    }

}
