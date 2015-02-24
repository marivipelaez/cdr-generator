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

class DailySimulation (simulatedData: String, sim: Simulator) {
  
  def simulation() {
    this.simulatedData match {
      case "users-cdrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val (users, cdrs) = this.sim.simulateUsersAndCdrs(new DateTime)
    
        header.union(cdrs.map(_.toString)).saveAsTextFile(new DateTime().toString(CDR.DateTimeFormat) + "_simulator.txt")
        users.map(_.toString()).saveAsTextFile(new DateTime().toString(CDR.DateTimeFormat) + "_users_simulator.txt") 
      }
      case "ddrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultDDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val ddrs = this.sim.simulateDdrs(new DateTime)
        
        header.union(ddrs.map(_.toString)).saveAsTextFile(new DateTime().toString(DDR.DateTimeFormat) + "_ddrs_simulator.txt")
      }
      case "users-ddrs" => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val (users, ddrs) = this.sim.simulateUsersAndDdrs(new DateTime)
    
        header.union(ddrs.map(_.toString)).saveAsTextFile(new DateTime().toString(DDR.DateTimeFormat) + "_ddrs_simulator.txt")
        users.map(_.toString()).saveAsTextFile(new DateTime().toString(DDR.DateTimeFormat) + "_users_simulator.txt") 
      }
      case "all" => {
        val cdrHeader: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val ddrHeader: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultDDR.header(DDR.FieldSeparator).split(DDR.FieldSeparator).map(_.toString))
        val (users, cdrs, ddrs) = this.sim.simulateUsersCdrsAndDdrs(new DateTime)
        
        cdrHeader.union(cdrs.map(_.toString)).saveAsTextFile(new DateTime().toString(CDR.DateTimeFormat) + "_simulator.txt")
        ddrHeader.union(ddrs.map(_.toString)).saveAsTextFile(new DateTime().toString(DDR.DateTimeFormat) + "_ddrs_simulator.txt")
        users.map(_.toString()).saveAsTextFile(new DateTime().toString(DDR.DateTimeFormat) + "_users_simulator.txt") 
      }
      case _ => {
        val header: org.apache.spark.rdd.RDD[String] = sc.parallelize(DefaultCDR.header(CDR.FieldSeparator).split(CDR.FieldSeparator).map(_.toString))
        val cdrs = this.sim.simulate(new DateTime)
        
        header.union(cdrs.map(_.toString)).saveAsTextFile(new DateTime().toString(CDR.DateTimeFormat) + "_simulator.txt")
      }
    }
  }
}