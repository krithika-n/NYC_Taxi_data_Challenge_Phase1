package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((Contains_function(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((Contains_function(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((Within_function(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((Within_function(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def Contains_function(queryRectangle:String, pointString:String):Boolean={
    /** Returns true if pointString1 lies inside or on hte boundary of queryRectangle
      *
      *@param queryRectangle diagonal point co-ordinates of the rectangle
      *@param pointString x and y co-ordinates of the point
      *@return true if point is contained within the rectangle
      */
    try {
          // var rect_array = new Array[String](4)
          val rect_array = queryRectangle.split(",")
          val r_x1 = rect_array(0).trim.toDouble
          val r_y1 = rect_array(1).trim.toDouble
          val r_x2 = rect_array(2).trim.toDouble
          val r_y2 = rect_array(3).trim.toDouble
            
          var pt_array = new Array[String](2)
          pt_array= pointString.split(",")          
          val pt_x=pt_array(0).trim.toDouble
          val pt_y=pt_array(1).trim.toDouble
          
          
          val min_x=math.min(r_x1,r_x2)
          val max_x=math.max(r_x1,r_x2)

          val min_y = math.min(r_y1, r_y2)
          val max_y = math.max(r_y1, r_y2)
          
          if(pt_y > max_y || pt_y < min_y || pt_x > max_x || pt_x < min_x)
            return false
          else
            return true
        }
        catch {
            case _: Throwable => return false
        }
  }

  def Within_function(pointString1:String, pointString2:String, distance:Double):Boolean={
    /** Returns true if the euclidean distance between pointString1 and pointString2 is <= distance
      *
      *@param pointString1 diagonal point co-ordinates of the rectangle
      *@param pointString x and y co-ordinates of the point
      *@return true if distance between points is <= given distance
      */
    try {
          // var pt1_array = new Array[String](2)
          val pt1_array = pointString1.split(",")

          val pt1_x= pt1_array(0).trim.toDouble
          val pt1_y= pt1_array(1).trim.toDouble
      
          var pt2_array = new Array[String](2)
          pt2_array = pointString2.split(",")

          val pt2_x=pt2_array(0).trim.toDouble
          val pt2_y=pt2_array(1).trim.toDouble
          
          val calc_Distance = Math.sqrt(Math.pow((pt1_x - pt2_x), 2) + Math.pow((pt1_y - pt2_y), 2))
          
          if(calc_Distance <= distance)
            return true 
          else
            return false
        }
        catch {
            case _: Throwable => return false
        }
  }
}
