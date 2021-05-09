package observatory

import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}
import observatory.Interaction.{Input, generateImage, generateTiles}
import observatory.Manipulation.{average, makeGrid}
import observatory.Visualization2.visualizeGrid
import com.sksamuel.scrimage.writer



object Main extends App {
  //println(locateTemperatures(1975, "/stations.csv", "/1975.csv"))
  //println(locationYearlyAverageRecords(locateTemperatures(1975, "/stations.csv", "/1975.csv")))
  //println(distanceEarth(60, 90, 20, 10))
  val known_temperatures: Iterable[(Location, Temperature)] = Seq(
    (Location(-20.0, 180.0), 0),
    (Location(-90.0, 100.0), 20),
    (Location(10.0, 30.0), 40)
  )

  val color_scale: Seq[(Temperature, Color)] = Seq(
    (60.0, Color(255,255,255)),
    (32.0, Color(255,0,0)),
    (12.0, Color(255,255,0)),
    (0.0, Color(0,255,255)),
    (-15.0, Color(0,0,255)),
    (-27.0, Color(255,0,255)),
    (-50.0, Color(33,0,107)),
    (-60.0, Color(0,0,0))
  )



  //val color_scale_RDD: RDD[(Temperature, Color)] = sparkSession.sparkContext.parallelize(color_scale)
  val temperature: Temperature = 0.25

  val test_tile: Tile = Tile(0,0,0)

  //tile(known_temperatures, color_scale, test_tile).output(new java.io.File("target/temperatures/2015/<zoom>/<x>-<y>.png"))


  //println(tile_plane(3).sortBy(tile => (tile.x, tile.y, tile.zoom)))
  //println(tile_scaffold(3).toSeq.sortBy(tile => (tile.zoom,tile.x, tile.y )))

  val input: Input = (locationYearlyAverageRecords(locateTemperatures(2015, "/stations.csv", "/2015.csv")),color_scale)

  val Average_1975_1990 = average((1975 to 1990).map(year => locationYearlyAverageRecords(locateTemperatures(year, "/stations.csv", s"/${year}.csv"))))
  //visualize(input._1, input._2).output(new java.io.File("target/2015-image.png"))
  //tile(input._1, input._2, test_tile).output(new java.io.File("target/test-tile-2015.png"))

  //generateTiles(Iterable((2015: Year, input)), generateImage)
  val t1 = System.nanoTime

  val gridImage = visualizeGrid(makeGrid(input._1), color_scale, test_tile)

  val duration = (System.nanoTime - t1) / 1e9d

  println(duration)

}
