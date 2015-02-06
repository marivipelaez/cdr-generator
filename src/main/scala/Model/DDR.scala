package model

import com.github.nscala_time.time._
import com.github.nscala_time.time.Imports._

import java.util.Random


/**
 * Companion object of DDR with some static definitions
 */
object DDR {
  val DateTimeFormat  = StaticDateTimeFormat.forPattern("yyyyMMddhhmmss")
  val FieldSeparator = ","
}

/** Represent a Data connection Detailed Record (DDR)
 * @param user                  User who connects
 * @param date                  The date of the connection
 * @param uplink                Number of bytes in uplink
 * @param downlink              Number of bytes in downlink
 * @param uplinkRoaming         Number of bytes in uplink in roaming
 * @param downlinkRoaming       Number of bytes in downlink in roaming
 * @param valueUplink           Amount of money (milli€ or milli$) in uplink
 * @param valueDownlink         Amount of money (milli€ or milli$) in downlink
 * @param valueUplinkRoaming    Amount of money (milli€ or milli$) in uplink in roaming
 * @param valueDownlinkRoaming  Amount of money (milli€ or milli$) in downlink in roaming
 */
class DDR(
	val user: User,
	val date: DateTime,
	val uplink: Int,
	val downlink: Int,
	val uplinkRoaming: Int,
	val downlinkRoaming: Int,
	val valueUplink: Int,
	val valueDownlink: Int,
	val valueUplinkRoaming: Int,
	val valueDownlinkRoaming: Int
) extends Serializable {

	/** DDR fields separate by a "," in the same order has the header
	 *
	 * @return  String
	 */
	override def toString(): String = this.toString(DDR.FieldSeparator)

	/** DDR fields separate by separator in the same order has the header
	 * @param separator		The separator
	 * @return  String		The concatenation of the ddr fields
	 */
	def toString(separator: String): String ={
		val tmp = this.toMap().values.mkString(separator)
		println(tmp)
		tmp
	}

	/** Concatenation of the DDR fields that will be returned by toString
	 *
	 * @param  separator String
	 * @return           String
	 */
	def header(separator: String): String = this.toMap().keys.mkString(separator)

	private def toMap() = {
		Map(
			"PHONE_NUMBER" -> user.id.toString,
			"DATE" -> date.toString(DDR.DateTimeFormat),
      "CELL_ID" -> user.where(date).id.toString(),
			"UPLINK" -> uplink,
			"DOWNLINK" -> downlink,
			"UPLINK_ROAMING" -> uplinkRoaming,
      "DOWNLINK_ROAMING" -> downlinkRoaming,
			"VALUE_UPLINK" -> valueUplink,
      "VALUE_DOWNLINK" -> valueDownlink,
			"VALUE_UPLINK_ROAMING" -> valueUplinkRoaming,
      "VAULE_DOWNLINK_ROAMING" -> valueDownlinkRoaming
		)
	}
}

/** Defaulft DDR for testing
 */
object DefaultDDR extends DDR(
	DefaultDumUser,
	DateTime.now,
	100,
	500,
	0, 0, 0, 0, 0, 0
)
