package observatory

import com.sksamuel.scrimage
import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Extraction.sparkSession
import org.apache.spark.rdd.RDD

import scala.collection.parallel.ParIterable

/**
  * 2nd milestone: basic visualization
  */



object Visualization extends VisualizationInterface {



  val INVERSE_POWER = 6

  def distanceEarth(longitude1: Double, longitude2: Double, latitude1: Double, latitude2: Double): Double = {
    val earth_radius = 6371.009
    var central_angle = 0.0
    if(latitude1 == latitude2 && longitude1 == longitude2){
    }
    else if(latitude1 == -latitude2 && math.abs(longitude1-longitude2) == 180.0){
      central_angle = math.Pi
    }
    else{
      central_angle = math.acos(math.sin(latitude1.toRadians)*math.sin(latitude2.toRadians)
        + math.cos(latitude1.toRadians)*math.cos(latitude2.toRadians)*math.cos(math.abs(longitude1.toRadians-longitude2.toRadians)))
    }

    central_angle*earth_radius
  }

  def predictTemperature(temperatures: RDD[(Location, Temperature)], location: Location): Temperature = {
    val location_distances: RDD[(Location, Double)] = temperatures.map(r => (r._1, distanceEarth(r._1.lon, location.lon, r._1.lat, location.lat)))
    val too_close = location_distances.filter(r => math.abs(r._2) < 1.0).collect()

    var interpolated_value: Double = 0.0

    if(!too_close.isEmpty){
      interpolated_value = temperatures.filter(r => r._1 == too_close(0)._1).take(1)(0)._2
    }

    else{
      val distances = location_distances.map(r => r._2)
      val inverse_distance_weights: RDD[Double] = distances.map(r => 1.0/math.pow(r,INVERSE_POWER))
      val summed_weights: Double = inverse_distance_weights.sum

      interpolated_value = temperatures.map(r => r._2)
        .zip(inverse_distance_weights)
        .map(r => r._1 * r._2)
        .sum/summed_weights
    }

    interpolated_value
  }


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    val temperatures_par = temperatures.par.seq.view
    val location_distances = temperatures_par.map(r => (r._1, distanceEarth(r._1.lon, location.lon, r._1.lat, location.lat)))
    val too_close = location_distances.filter(r => math.abs(r._2) < 1.0)

    var interpolated_value: Double = 0.0

    if(too_close.nonEmpty){
      interpolated_value = temperatures_par.filter(r => r._1 == too_close.head._1).head._2
    }

    else{

      val inverse_distance_weights =
        location_distances
        .map(r => 1.0/math.pow(r._2,INVERSE_POWER))

      val summed_weights: Double = inverse_distance_weights.sum

      interpolated_value = temperatures_par.map(r => r._2)
        .zip(inverse_distance_weights)
        .map(r => r._1 * r._2)
        .sum/summed_weights
    }

    interpolated_value

  }



  def lerp_color(v0: Color, v1: Color, t: Double): Color = {
    val red = (1-t)*v0.red + t*v1.red
    val green = (1-t)*v0.green + t*v1.green
    val blue = (1-t)*v0.blue + t*v1.blue

    Color(red.round.toInt, green.round.toInt, blue.round.toInt)
  }




  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {

    val asc_points = points.toSeq.sortBy(p => p._1)
    val asc_temperatures = asc_points.map(p => p._1)
    val lowest = asc_points.head
    val highest = asc_points.last

    val lerp: Color = {
      if(value <= lowest._1){
        lowest._2
      }
      else if(value >= highest._1){
        highest._2
      }
      else{

        val temperature_ranges = (Double.MinValue +: asc_temperatures).zip(asc_temperatures :+ Double.MaxValue)
        val valid_ranges: Seq[(Temperature, Temperature)] = temperature_ranges.filter(temp => temp._1 < value && value <= temp._2)

        if(valid_ranges.isEmpty){
          throw new NoSuchElementException(s"No valid temperature range was found for ${value}")
        }
        else{
          val (min, max) = valid_ranges.head
          val ratio = math.abs(value-min)/math.abs(max - min)
          lerp_color(points.filter(r => r._1 == min).head._2, points.filter(r => r._1 == max).head._2, ratio)
        }

      }
    }
    lerp

  }


  val HEIGHT: Int = 180
  val WIDTH: Int = 360
  def to_GPS_linear(x: Int, y: Int): Location = {
    Location.apply(-y+90, x-180)
  }


  def visualize(temperatures: RDD[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def image_cartesian_generation: Seq[(Int, Int)] = {
      val xy_seq: Seq[(Int, Int)] = for( y <- 0 until HEIGHT; x <- 0 until WIDTH) yield {
        (x,y)
      }
      xy_seq
    }

    //println(s"xy Pixel samples: ${xy_seq.take(100).map(xy => interpolateColor(colors, predictTemperature(temperatures, to_latlon(xy._1, xy._2))).toString)}")

    def temperatureMap_generation(image_cartesian_generation: Seq[(Int, Int)],cartesian_to_GPS: (Int, Int) => Location = to_GPS_linear): Image = {

      val xy_seq = image_cartesian_generation

      val xy_color = xy_seq
        .map(xy => interpolateColor(colors, predictTemperature(temperatures, cartesian_to_GPS(xy._1,xy._2))))
        .map(xy_color => Pixel.apply(xy_color.red, xy_color.green, xy_color.blue,255))

      val pixels_RDD: Array[Pixel] = xy_color.toArray


      val image = scrimage.Image.apply(WIDTH, HEIGHT, pixels_RDD)

      image
    }

    temperatureMap_generation(image_cartesian_generation)
  }






  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def image_cartesian_generation: Seq[(Int, Int)] = {
      val xy_seq: Seq[(Int, Int)] = for( y <- 0 until HEIGHT; x <- 0 until WIDTH) yield {
        (x,y)
      }
      xy_seq
    }

    //println(s"xy Pixel samples: ${xy_seq.take(100).map(xy => interpolateColor(colors, predictTemperature(temperatures, to_latlon(xy._1, xy._2))).toString)}")

    def temperatureMap_generation(image_cartesian_generation: Seq[(Int, Int)],cartesian_to_GPS: (Int, Int) => Location = to_GPS_linear): Image = {
      val xy_RDD = sparkSession.sparkContext.parallelize(image_cartesian_generation)


      val xy_color_RDD = xy_RDD
        .map(xy => interpolateColor(colors, predictTemperature(temperatures, cartesian_to_GPS(xy._1,xy._2))))
        .map(xy_color => Pixel.apply(xy_color.red, xy_color.green, xy_color.blue,255))

      val pixels_RDD: Array[Pixel] = xy_color_RDD.collect()


      val image = scrimage.Image.apply(WIDTH, HEIGHT, pixels_RDD)

      image
    }

    temperatureMap_generation(image_cartesian_generation)
  }


}

