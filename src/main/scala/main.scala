import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
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


object CDRSimulation{
  val usage = """
    Usage: CDRSimulation [--data (cdrs, ddrs, users-cdrs, users-ddrs, all)] [--num-users num] [--interval num] [--num-cells num]

     * data identifies which kind of data is to be simulated. If not present only cdrs will be simulated.
     * num-users shows the number of users in the simulation. By default, 50 users will be generated.
     * interval number of days in the simulation, from the present day to interval-days in the past. 
                If not present a single day will be generated.
     * num-cells number of cells generated. By default, 10 cells will be used.
  """
  val validData: List[String] = List("cdrs", "ddrs", "users-cdrs", "users-ddrs", "all")
  
	def main(args: Array[String]){
    
    // Get args 
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]
    
    def nextOption(map: OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--data" :: value :: tail => nextOption(map ++ Map('data -> value.toString), tail)
        case "--num-users" :: value :: tail => nextOption(map ++ Map('numusers -> value.toInt), tail)
        case "--interval" :: value :: tail => nextOption(map ++ Map('interval -> value.toInt), tail)
        case "--num-cells" :: value :: tail => nextOption(map ++ Map('numcells -> value.toInt), tail)
        case _ :: tail => nextOption(map, tail)
      }
    }
    
    val options = nextOption(Map(), arglist)
    
    println(options)
    
    val simulatedData = options.getOrElse('data, null).asInstanceOf[String]
    val numUsers = options.getOrElse('numusers, 50).asInstanceOf[Int]
    val interval = options.getOrElse('interval, 1).asInstanceOf[Int]
    val numCells = options.getOrElse('numcells, 10).asInstanceOf[Int]
    
    if (simulatedData != null && !validData.contains(simulatedData)){
      println(usage)
      System.exit(0)
    }
    
    // Create the simulator of your choice. If it does not exist, define it!
		val sim = new NormalSimulator(
			new BasicCellsGenerator(numCells),
			new HarcodedOperatorsGenerator(),
			new BasicSpanishUsersGenerator(numUsers),
			new RandomSocialNetworkGenerator()
		)
    
    // Run a simulation from today till interval days in the past
    val simulation = new DailySimulation(simulatedData, sim)
    val simDate = new DateTime
    for (i <- 0 until interval) {
      simulation.simulation(simDate - i.days) 
    } 
    
    sc.stop()
	}
}
