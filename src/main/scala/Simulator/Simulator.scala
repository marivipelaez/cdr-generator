package simulator

import com.github.nscala_time.time.Imports._

import java.util.Random
import org.apache.spark.graphx._

import spark.Spark.sc
import generator.cells._
import generator.operators._
import generator.users._
import generator.socialnetwork._
import model._

abstract class Simulator(
	val cellsGenerator: CellsGenerator,
	val operatorsGenerator: OperatorsGenerator,
	val usersGenerator: UsersGenerator,
  val socialNetworkGenerator: SocialNetworkGenerator
) extends Serializable {
  
    
  val users: org.apache.spark.rdd.RDD[User]
  val socialNetwork: Graph[User, Relation]
  
  /** Generate one day of CDR based on the results of the other generators
   *  
   * @param  day 
   * @return the generated CDRs for the day
   */
  def simulate(day: DateTime) : org.apache.spark.rdd.RDD[CDR]
  
	/** Generate one day of CDR based on the results of the other generators
   *  
	 * @param  day 
	 * @return a tuple with two elements: the generated users and CDRs for the day
	 */
	def simulateUsersAndCdrs(day: DateTime) : (org.apache.spark.rdd.RDD[User], org.apache.spark.rdd.RDD[CDR])
  
  /** Generate one day of CDR based on the results of the other generators
   *  
   * @param  day 
   * @return the generated DDRs for the day
   */
  def simulateDdrs(day: DateTime) : org.apache.spark.rdd.RDD[DDR]
  
  /** Generate one day of DDR based on the results of the other generators
   *  
   * @param  day 
   * @return a tuple with two elements: the generated users and DDRs for the day
   */
  def simulateUsersAndDdrs(day: DateTime) : (org.apache.spark.rdd.RDD[User], org.apache.spark.rdd.RDD[DDR])
  
  /** Generate one day of CDR and DDR based on the results of the other generators
   *  
   * @param  day 
   * @return a tuple with three elements: the generated users, the CDRs and DDRs for the day
   */
  def simulateUsersCdrsAndDdrs(day: DateTime) : 
    (org.apache.spark.rdd.RDD[User], 
     org.apache.spark.rdd.RDD[CDR], 
     org.apache.spark.rdd.RDD[DDR])

}


class BasicSimulator(
    override val cellsGenerator: CellsGenerator,
    override val operatorsGenerator: OperatorsGenerator,
    override val usersGenerator: UsersGenerator,
    override val socialNetworkGenerator: SocialNetworkGenerator
    ) extends Simulator(cellsGenerator, operatorsGenerator, usersGenerator, socialNetworkGenerator) {
  
	protected val rand = new Random
  val operators = operatorsGenerator.generate()
  val cells = cellsGenerator.generate(operators)
  override val users = usersGenerator.generate(cells, operators)
  override val socialNetwork = socialNetworkGenerator.generate(users)

  
	override def simulate(day: DateTime) : org.apache.spark.rdd.RDD[CDR] = {
    
		val cdrs = generateCDRs(users, socialNetwork, operators, cells, day)
    
	  cdrs
	}
  
  override def simulateUsersAndCdrs(day: DateTime):
  (org.apache.spark.rdd.RDD[User], org.apache.spark.rdd.RDD[CDR]) = {

    val cdrs = generateCDRs(users, socialNetwork, operators, cells, day)
    
    (users, cdrs)
  }
  
  override def simulateDdrs(day: DateTime): org.apache.spark.rdd.RDD[DDR] = {
    
    val ddrs = generateDDRs(users, operators, cells, day)
    
    ddrs
  }
  
  override def simulateUsersAndDdrs(day: DateTime): (
      org.apache.spark.rdd.RDD[User], 
      org.apache.spark.rdd.RDD[DDR]
      ) = {
    
    val ddrs = generateDDRs(users, operators, cells, day)
    
    (users, ddrs)
  }
  
  override def simulateUsersCdrsAndDdrs(day: DateTime): (
      org.apache.spark.rdd.RDD[User], 
      org.apache.spark.rdd.RDD[CDR],
      org.apache.spark.rdd.RDD[DDR]
      ) = {
    
    val cdrs = generateCDRs(users, socialNetwork, operators, cells, day)
    val ddrs = generateDDRs(users, operators, cells, day)
    
    (users, cdrs, ddrs)
  }
  
  def generateCDRs(
      users: org.apache.spark.rdd.RDD[User], 
      socialNetwork: Graph[User, Relation],
      operators: Array[Operator], 
      cells: Array[Cell],
      day: DateTime
      ): org.apache.spark.rdd.RDD[CDR] = {
    
    val cdrs = socialNetwork.edges.flatMap{ 
      case Edge(a, b, Relation(userA, userB))=>
        val numberOfCDR = rand.nextInt(10)
        (0 to numberOfCDR).map(_ =>
          randomCDR(userA, userB, day)
        )
    }
    val nCdr = cdrs.count

    val central = new DumUser( -1, operators(0), cells(0) )
    val centralCdrs = users.sample(false, rand.nextDouble).flatMap{ u =>
        val nOfCentralCDR = rand.nextInt(3)
        (0 to nOfCentralCDR).map(_ => 
            randomCentralCDR(u, central, day)
            )
    }
    (cdrs ++ centralCdrs)
  }

  def generateDDRs(
      users: org.apache.spark.rdd.RDD[User],
      operators: Array[Operator], 
      cells: Array[Cell],
      day: DateTime
      ): org.apache.spark.rdd.RDD[DDR] = {
    
    val ddrs = users.sample(false, rand.nextDouble).flatMap{ u =>
        val nDdrs = rand.nextInt(10)
        0 to nDdrs map(_ => randomDDR(u, day))
      }
    
    ddrs
  }
  
	def randomCDR(userA: User, userB: User, day: DateTime): CDR = {
		val date = day.hour(rand.nextInt(11)+1).
		withSecondOfMinute(rand.nextInt(59)+1)
		val cdrType = if(rand.nextDouble < 0.5) SMS else Call
		val duration = if(cdrType == SMS) 0 else rand.nextInt(1000)
		val cellA = userA.where(date)
		val cellB = userB.where(date)
		val terminationStatusA = if(cellA.drop) RingOff else Drop
		val terminationStatusB = if(cellB.drop) RingOff else Drop
		new CDR(
			userA,
			userB,
			cellA,
			cellB,
			date,
			duration,
			cdrType,
			terminationStatusA,
			terminationStatusB,
			userA.callerCost(userB, date, duration, cdrType),
			userB.callerCost(userA, date, duration, cdrType),
			TransitType.randomTransitType,
			userA.tac(date),
			userB.tac(date)
			
		)
	}

	def randomCentralCDR(user: User, central: User, day: DateTime): CDR = {
		val date = day.hour(rand.nextInt(11)+1).withSecondOfMinute(rand.nextInt(59)+1)
		val cellA = central.where(date)
		val cellB = user.where(date)
		val terminationStatusA = if(cellA.drop) RingOff else Drop
		val terminationStatusB = if(cellB.drop) RingOff else Drop
		new CDR(
			central,
			user,
			cellA,
			cellB,
			date,
			0,
			SMS,
			terminationStatusA,
			terminationStatusB,
			0,
			0,
			CallCenter,
			central.tac(date),
			user.tac(date)
			
		)
	}
  
  def randomDDR(user: User, day: DateTime): DDR = {
    val date = day.hour(rand.nextInt(11)+1).withSecondOfMinute(rand.nextInt(59)+1)
    
    // By default, no user is in roaming, and all traffic is included in the tariff
    new DDR(user, date, rand.nextInt(100), rand.nextInt(500), 0, 0, 0, 0, 0, 0)
  }
}



