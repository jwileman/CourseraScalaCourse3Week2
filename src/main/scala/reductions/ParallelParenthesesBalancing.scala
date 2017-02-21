package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 10000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def isBalanced(chars: Array[Char], openCount: Int): Boolean = {
      if (chars.isEmpty)
        openCount == 0
      else if (chars.head == '(')
        isBalanced(chars.tail, openCount + 1)
      else if (chars.head == ')')
        openCount > 0 && isBalanced(chars.tail, openCount - 1)
      else
        isBalanced(chars.tail, openCount)
    }
    isBalanced(chars, 0)
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    /**
      * sequential traversal
      */
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      var openCount = if (arg1 > arg2) arg1 - arg2 else 0
      var closedCount = if (arg1 < arg2) arg2 - arg1 else 0

      var i = idx

      while (i < until) {
        if (chars(i) == '(') {
          openCount = openCount + 1
        } else if (chars(i) == ')') {
          if (openCount > 0) {
            openCount = openCount - 1
          } else {
            closedCount = closedCount + 1
          }
        }
        i = i + 1
      }

      (openCount, closedCount)
    }

    /**
      * parallel reduction
      */
    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = (from + until) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))

        if (left._1 < right._2) {
          (right._1, right._2 + left._2 - left._1)
        } else if (left._1 > right._2) {
          (left._1 + right._1 - right._2, left._2)
        } else {
          (right._1, left._2)
        }
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

}
