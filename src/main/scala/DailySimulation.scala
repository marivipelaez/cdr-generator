import org.apache.spark.SparkContext._
import com.github.nscala_time.time._
import com.github.nscala_time.time.Imports._
import spark.Spark.sc
import generator.users._
import generator.operators._
import generator.cells._
import generator.socialnetwork._
import simulator._
import model.DefaultCDR
import model.DefaultDDR
import model.CDR
import model.DDR

/**
 * Runs a simulation of data for a day using the given simulator
 * 
 * @param data: String: type of simulation. One of:
 *  * users-cdrs: to simulate cdrs and store both cdrs and users, 
 *  * ddrs: to simulate and store only ddrs,
 *  * users-ddrs: to simulate ddrs and store both cdrs and users,
 *  * all: to simulate and store cdrs, ddrs and users. 
 *  * Otherwise, only cdrs are simulated and stored
 * @param simulator: simulator.Simulator: defines the way of generating operators, cells, users, 
 * social network and the data distribution.  
 */
class DailySimulation (val data: String, val simulator: Simulator) { 
  
  /**
   * Runs a simulation of data for a day using the given simulator
   * 
   * @param: day: DayTime: day to be simulated.
   * 
   * @return: This function returns nothing, but its result is stored in:
   * * project_home/simulations/yyyyMMddd_cdrs
   * * project_home/simulations/yyyyMMddd_ddrs
   * * project_home/simulations/yyyyMMddd_users
   * 
   */
  def simulation(day: DateTime) {
    val formattedDate = day.toString(DailySimulation.DateFormat)
    data match {
      case "users-cdrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val (users, cdrs) = simulator.simulateUsersAndCdrs(day)
    
        header.union(cdrs.map(_.toString)).saveAsTextFile(DailySimulation.cdrsSimulation.format(formattedDate))
        users.map(_.toString()).saveAsTextFile(DailySimulation.usersSimulation.format(formattedDate)) 
      }
      case "ddrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultDDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val ddrs = simulator.simulateDdrs(day)
        
        header.union(ddrs.map(_.toString)).saveAsTextFile(DailySimulation.ddrsSimulation.format(formattedDate))
      }
      case "users-ddrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val (users, ddrs) = simulator.simulateUsersAndDdrs(day)
    
        header.union(ddrs.map(_.toString)).saveAsTextFile(DailySimulation.ddrsSimulation.format(formattedDate))
        users.map(_.toString()).saveAsTextFile(DailySimulation.usersSimulation.format(formattedDate)) 
      }
      case "all" => {
        val cdrHeader: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val ddrHeader: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultDDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val (users, cdrs, ddrs) = simulator.simulateUsersCdrsAndDdrs(day)
        
        cdrHeader.union(cdrs.map(_.toString)).saveAsTextFile(DailySimulation.cdrsSimulation.format(formattedDate))
        ddrHeader.union(ddrs.map(_.toString)).saveAsTextFile(DailySimulation.ddrsSimulation.format(formattedDate))
        users.map(_.toString()).saveAsTextFile(DailySimulation.usersSimulation.format(formattedDate)) 
      }
      case _ => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val cdrs = simulator.simulate(day)
        
        header.union(cdrs.map(_.toString)).saveAsTextFile(DailySimulation.cdrsSimulation.format(formattedDate))
      }
    }
  }
}

object DailySimulation {
  
  val DateFormat = StaticDateTimeFormat.forPattern("yyyyMMdd")
  val cdrsSimulation = "simulations/%s_cdrs"
  val ddrsSimulation = "simulations/%s_ddrs"
  val usersSimulation = "simulations/%s_users"
  
}