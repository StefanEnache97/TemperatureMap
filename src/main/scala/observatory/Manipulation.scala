package observatory

import observatory.Visualization.predictTemperature

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
    var computed: HashMap[(GridLocation, Iterable[(Location, Temperature)]), Temperature] = new HashMap()

  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    grid: GridLocation => {
      if(computed.contains((grid, temperatures))){
        computed((grid, temperatures))
      }
      else{
        val predicted = predictTemperature(temperatures, Location(grid.lat.toDouble, grid.lon.toDouble))
        computed = computed + ((grid, temperatures) -> predicted)
        predicted
      }
//      predictTemperature(temperatures, Location(grid.lat.toDouble, grid.lon.toDouble))

    }

  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    grid: GridLocation => {
      val running_average = temperaturess
        .map(temp => makeGrid(temp)(grid))
        .aggregate((0: Int, 0.0: Double))((agg, temp) => (agg._1 + 1, agg._2 + temp), (agg1, agg2) => (agg1._1 + agg2._1, agg1._2 + agg2._2))
      running_average._2/running_average._1
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    grid: GridLocation => {
      makeGrid(temperatures)(grid) - normals(grid)
    }
  }


}

