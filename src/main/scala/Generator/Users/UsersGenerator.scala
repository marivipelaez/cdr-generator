package generator.users

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Random
import scala.util.Random.nextInt

import model._
import spark.Spark.sc

abstract class UsersGenerator() extends Serializable{
	def generate(
		cells: Array[Cell],
		operators: Array[Operator] 
		) : org.apache.spark.rdd.RDD[User]
}

class BasicUsersGenerator(nUsers: Int) extends UsersGenerator {
	protected val rand = new Random

	override def generate(
		cells: Array[Cell],
		operators: Array[Operator] 
		) : org.apache.spark.rdd.RDD[User] = {

			sc.parallelize( 1 to nUsers map{ id => 
				new DumUser( id, operators(0), sampleCells(cells)(0) )
			})

	}

	def sampleCells(cells: Array[Cell]): Array[Cell] = {
		val size = rand.nextInt(cells.length);
		val set = Set( 0 to size map{_ =>
				cells(rand.nextInt(cells.length))
			} :_*)
		set.toArray
	}
}

/**
 * Generates an array of Spanish mobile numbers.
 * 
 * @param nUsers        Number of random users to be generated
 */
class BasicSpanishUsersGenerator(nUsers: Int) extends UsersGenerator {
  protected val rand = new Random

  override def generate(
    cells: Array[Cell],
    operators: Array[Operator] 
    ) : org.apache.spark.rdd.RDD[User] = {
     
      sc.parallelize( spanishRandomUsers(nUsers) map{ id => 
        new DumUser( id, operators(0), sampleCells(cells)(0) )
      })

  }

  def sampleCells(cells: Array[Cell]): Array[Cell] = {
    val size = rand.nextInt(cells.length);
    val set = Set( 0 to size map{_ =>
        cells(rand.nextInt(cells.length))
      } :_*)
    set.toArray
  }
  
  def spanishRandomUsers(nUsers: Int): Array[Int] = {
    val rangeNumbers = 600000000 to 699999999
    val selectedNumbers = new Array[Int](nUsers)
    
    0 until nUsers foreach { x => selectedNumbers(x) = {
      val rand = new scala.util.Random(x*System.currentTimeMillis())
      val random_index =  rand.nextInt(rangeNumbers.length)
      rangeNumbers(random_index) }
    }
    selectedNumbers
  }
}

