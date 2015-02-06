package simulator

import com.github.nscala_time.time.Imports._

import java.util.Random
import org.apache.spark.graphx._
import breeze.stats.distributions.Gaussian
import org.joda.time.Interval

import spark.Spark.sc
import generator.cells._
import generator.operators._
import generator.users._
import generator.socialnetwork._
import model._

class NormalSimulator(
    override val cellsGenerator: CellsGenerator,
    override val operatorsGenerator: OperatorsGenerator,
    override val usersGenerator: UsersGenerator,
    override val socialNetworkGenerator: SocialNetworkGenerator
    ) extends BasicSimulator(cellsGenerator, operatorsGenerator, usersGenerator, socialNetworkGenerator) {

  val CALL_INTERVAL_START_HOUR = 8
  val LENGTH_CALL_INTERVAL_DAY = 57600
  
  override def generateCDRs(
      users: org.apache.spark.rdd.RDD[User], 
      socialNetwork: Graph[User, Relation],
      operators: Array[Operator], 
      cells: Array[Cell],
      day: DateTime
      ): org.apache.spark.rdd.RDD[CDR] = {
    
    val cdrs = socialNetwork.edges.flatMap{ 
      case Edge(a, b, Relation(userA, userB))=>
        val numberOfCDR = rand.nextInt(10)
        val userMu = rand.nextInt(300)
        (0 to numberOfCDR).map(_ =>
          normalCDR(userA, userB, day, userMu)
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

  override def generateDDRs(
      users: org.apache.spark.rdd.RDD[User],
      operators: Array[Operator], 
      cells: Array[Cell],
      day: DateTime
      ): org.apache.spark.rdd.RDD[DDR] = {
    
    val ddrs = users.sample(false, rand.nextDouble).flatMap{ u =>
        val nDdrs = rand.nextInt(10)
        val userUplinkMu = rand.nextInt(100)
        val userDownlinkMu = rand.nextInt(500)
        0 to nDdrs map(_ => normalDDR(u, day, userUplinkMu, userDownlinkMu))
      }
    
    ddrs
  }
  
	def normalCDR(userA: User, userB: User, day: DateTime, userMu: Int): CDR = {
		val date = randomTime(day)
		val cdrType = if(rand.nextDouble < 0.5) SMS else Call
		val duration = if(cdrType == SMS) 0 else Gaussian(userMu, 0).sample().toInt
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
  
  def normalDDR(user: User, day: DateTime, userUplinkMu: Int, userDownlinkMu: Int): DDR = {
    val date = randomTime(day)
    
    // By default, no user is in roaming, and all traffic is included in the tariff
    new DDR(user, date, Gaussian(userUplinkMu, 0).sample().toInt, Gaussian(userDownlinkMu, 0).sample().toInt, 
        0, 0, 0, 0, 0, 0)
  }
  
  def randomTime(day: DateTime): DateTime = {
    val initialDate = day.hour(CALL_INTERVAL_START_HOUR)
    initialDate.plusSeconds(rand.nextInt(LENGTH_CALL_INTERVAL_DAY)) 
    
  }
}



