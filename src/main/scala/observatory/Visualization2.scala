package observatory

import com.sksamuel.scrimage
import com.sksamuel.scrimage.{Image, Pixel}

import observatory.Interaction.tileLocation
import observatory.Visualization.interpolateColor
import java.math.BigDecimal


/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    val y1 = d00*(1-point.x) + d10*point.x
    val y2 = d01*(1-point.x) + d11*point.x
    y1*(1-point.y) + y2*point.y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {

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


      def interpolatedTemperature(grid: GridLocation => Temperature, sub_tile: Tile): Temperature = {
        val lat_lon = tileLocation(sub_tile)

        val lat = new BigDecimal(String.valueOf(lat_lon.lat))
        val latInt: Int = lat.intValue
        val latDec: Double = lat.subtract(new BigDecimal(latInt)).doubleValue

        val lon = new BigDecimal(String.valueOf(lat_lon.lon))
        val lonInt: Int = lon.intValue
        val lonDec: Double = lon.subtract(new BigDecimal(lonInt)).doubleValue

        val d00 = grid(GridLocation(latInt,lonInt))
        val d10 = grid(GridLocation(latInt,lonInt + 1))
        val d01 = grid(GridLocation(latInt + 1,lonInt))
        val d11 = grid(GridLocation(latInt + 1,lonInt + 1))

        bilinearInterpolation(CellPoint(lonDec, latDec), d00, d01, d10, d11)

      }

      val sub_tile_par = sub_tiles.par

      val tile_color_par = sub_tile_par.seq.view
        .map(sub_tile => interpolateColor(colors, interpolatedTemperature(grid, sub_tile)))
        .map(sub_tile_color => Pixel.apply(sub_tile_color.red, sub_tile_color.green, sub_tile_color.blue,127))

      val pixels_arr: Array[Pixel] = tile_color_par.toArray


      val image = scrimage.Image.apply(WIDTH, HEIGHT, pixels_arr)

      image
    }

    temperatureMap_generation


  }

}
