package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  /*
    Return the square of the value
  */
  def squared_val(x:Int):Double={ (x*x).toDouble; }
  
  /* Return count of # of neighbour cells
  */
  def countNeighbours(min_x: Int, min_y: Int, min_z: Int, max_x: Int, max_y: Int, max_z: Int, ip_x: Int, ip_y: Int, ip_z: Int): Int = 
  {
    var cells_count = 0;
    val val1 = 7
    val val2 =11
    val val3 =17
    val val4 =26
      
    if (ip_x == min_x || ip_x == max_x) {
      cells_count += 1;
    }

    if (ip_y == min_y || ip_y == max_y) {
      cells_count += 1;
    }

    if (ip_z == min_z || ip_z == max_z) {
      cells_count += 1;
    }

    if (cells_count == 1) {
      return val3;
    } 
    else if (cells_count == 2)
    {
      return val2;
    }
    else if (cells_count == 3)
    {
      return val1;
    } 
    else
    {
      return val4;
    }
  }

  /* Return the Getis-Ord Statistic
  */
  def gettisordstatistic(x: Int, y: Int, z: Int, mean:Double, sd: Double, countn: Int, sumn: Int, numcells: Int): Double =
  {
    val num = (sumn.toDouble - (mean*countn.toDouble))
    val deno = sd*math.sqrt((((numcells.toDouble*countn.toDouble) -(countn.toDouble*countn.toDouble))/(numcells.toDouble-1.0).toDouble).toDouble).toDouble
    (num/deno).toDouble
  }

}
