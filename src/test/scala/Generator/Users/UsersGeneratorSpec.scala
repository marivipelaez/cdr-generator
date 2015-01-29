import org.scalatest.FlatSpec
import scala.util.Random
import scala.util.Random._
import org.joda.time.DateTime
import generator.users._
import model._

class UsersGeneratorSpec extends FlatSpec {
  
	def fixture = new {
		val operators = Array(DefaultOperator.asInstanceOf[Operator])
    val cells = Array(DefaultCell.asInstanceOf[Cell])
	}

	"BasicSpanishUsersGenerator" should "generate the right number of users" in {
		val f = fixture
		val nUsers = 50
		val basicSpanishUsersGenerator = new BasicSpanishUsersGenerator(50)
		assert( basicSpanishUsersGenerator.generate(f.cells, f.operators).count() == nUsers )
	}
  
	it should "generate users with a Spanish mobile number" in {
		val f = fixture
		val nUsers = 10
    val rangeNumbers = 600000000 to 699999999
		val basicSpanishUsersGenerator = new BasicSpanishUsersGenerator(nUsers)
		val usersRdd = basicSpanishUsersGenerator.generate(f.cells, f.operators)
    val users = usersRdd.collect()
    users.foreach{u => assert(
        u.id >= rangeNumbers.start || u.id <= rangeNumbers.end, 
        s"Mobile number : ${u.id} outside Spanish range : ${rangeNumbers.start} - ${rangeNumbers.end}"
        )
    } 
	}
}
