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
          val rect_array = queryRectangle.split(",").map(x=>x.trim.toDouble)
          val pt_array= pointString.split(",").map(x=>x.trim.toDouble) 

          def min_y(arr:Array[Double]):Double = (arr.indices.collect { case i if i % 2 == 1 => arr(i) }).min
          def min_x(arr:Array[Double]):Double = (arr.indices.collect { case i if i % 2 == 0 => arr(i) }).min
          def max_y(arr:Array[Double]):Double = (arr.indices.collect { case i if i % 2 == 1 => arr(i) }).max
          def max_x(arr:Array[Double]):Double = (arr.indices.collect { case i if i % 2 == 0 => arr(i) }).max
        
          
          val min_x=min_x(rect_array)
          val max_x=max_x(rect_array)

          val min_y = min_y(rect_array)
          val max_y = max_y(rect_array)
          
          if(pt_array(1) > max_y || pt_array(1) < min_y || pt_array(0) > max_x || pt_array(0) < min_x)
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
          val pt1 = pointString1.split(",").map(x=>x.trim.toDouble)
          val pt2 = pointString2.split(",").map(x=>x.trim.toDouble)
          
          val calc_Distance = Math.sqrt(Math.pow((pt1(0) - pt2(0)), 2) + Math.pow((pt1(1) - pt2(1)), 2))
          
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
