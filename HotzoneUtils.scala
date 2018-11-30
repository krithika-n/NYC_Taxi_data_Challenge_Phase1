package cse512

object HotzoneUtils {

  /** Returns true if pointString lies inside or on the boundary of queryRectangle
      *
      *@param queryRectangle diagonal point co-ordinates of the rectangle
      *@param pointString x and y co-ordinates of the point
      *@return true if point is contained within the rectangle
      */
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
     try {
          val rect_array = queryRectangle.split(",").map(x=>x.trim.toDouble)
          val pt_array= pointString.split(",").map(x=>x.trim.toDouble)
          
          val min_x=math.min(rect_array(0),rect_array(2))
          val max_x=math.max(rect_array(0),rect_array(2))

          val min_y = math.min(rect_array(1), rect_array(3))
          val max_y = math.max(rect_array(1), rect_array(3))
          
          !(pt_array(1) > max_y || pt_array(1) < min_y || pt_array(0) > max_x || pt_array(0) < min_x)
        }
        catch {
            case _: Throwable => return false
        }
  }
}