package observatory

import com.sksamuel.scrimage
import com.sksamuel.scrimage.{Image, Pixel}

import observatory.Visualization.{interpolateColor, predictTemperature}
import com.sksamuel.scrimage.writer

import scala.math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {

      val lat = toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / (1<<tile.zoom)))))

      val lon = tile.x.toDouble / (1<<tile.zoom) * 360.0 - 180.0

      Location(lat, lon)

  }


  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */


  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {

      val HEIGHT = 256
      val WIDTH = 256

      def sub_tile_plane(zoom: Int, x: Int = 0, y: Int = 0, counter: Int = 0): Seq[Tile] = {
        if(counter == 8){
          Seq(Tile(x, y, zoom))
        }
        else{
          sub_tile_plane(zoom, 2*x, 2*y, counter+1) ++ sub_tile_plane(zoom, 2*x + 1, 2*y, counter+1) ++ sub_tile_plane(zoom, 2*x, 2*y + 1, counter + 1) ++ sub_tile_plane(zoom, 2*x + 1, 2*y + 1, counter + 1)
        }
      }
      val sub_tiles = sub_tile_plane(tile.zoom + 8, tile.x, tile.y).sortBy(tile => (tile.y, tile.x, tile.zoom))


      def temperatureMap_generation: Image = {
        val tile_par = sub_tiles.par

        val tile_color_par = tile_par
          .map(tile => interpolateColor(colors, predictTemperature(temperatures, tileLocation(tile))))
          .map(tile_color => Pixel.apply(tile_color.red, tile_color.green, tile_color.blue,127))

        val pixels_RDD: Array[Pixel] = tile_color_par.toArray


        val image = scrimage.Image.apply(WIDTH, HEIGHT, pixels_RDD)

        image
      }

      temperatureMap_generation


  }

  def tile_plane(zoom: Int, x: Int = 0, y: Int = 0, counter: Int = 0): Seq[Tile] = {
    if(counter == zoom){
      Seq(Tile(x, y, zoom))
    }
    else{
      tile_plane(zoom, 2*x, 2*y, counter+1) ++ tile_plane(zoom, 2*x + 1, 2*y, counter+1) ++ tile_plane(zoom, 2*x, 2*y + 1, counter + 1) ++ tile_plane(zoom, 2*x + 1, 2*y + 1, counter + 1)
    }
  }

  def tile_scaffold(zoom_range: Int): Iterable[Tile] = {
    (0 to zoom_range).flatMap(zoom => tile_plane(zoom))

  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {

    val ZOOM_RANGE = 3


    yearlyData
      .foreach(year_data =>
        tile_scaffold(ZOOM_RANGE)
          .foreach(tile => generateImage(year_data._1, tile, year_data._2)))

  }

  type Input = (Iterable[(Location, Temperature)], Iterable[(Temperature, Color)])

  def generateImage(year: Year, tile_data: Tile, data: Input): Unit = {

    val dirs = new java.io.File(s"target/temperatures/${year}/${tile_data.zoom}")
    dirs.mkdirs()
    println(dirs.mkdirs())
    tile(data._1, data._2, tile_data).output(new java.io.File(s"target/temperatures/${year}/${tile_data.zoom}/${tile_data.x}-${tile_data.y}.png"))
    println(s"tile completed: ${tile_data}")

  }


}
