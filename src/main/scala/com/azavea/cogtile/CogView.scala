package com.azavea.cogtile

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.render.ColorRamps.Viridis
import geotrellis.raster.reproject._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.proj4._
import geotrellis.util.{ FileRangeReader, RangeReader }
import geotrellis.spark.tiling._
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.hack.GTHack.closestTiffOverview
import java.nio.file._
import java.net.URI


object CogView {

  private val TmsLevels: Array[LayoutDefinition] = {
    val scheme = ZoomedLayoutScheme(WebMercator, 256)
    for (zoom <- 0 to 64) yield scheme.levelForZoom(zoom).layout
  }.toArray

  def getRangeReader(uri: URI): Option[RangeReader] = {
    uri.getScheme match {
      case "file" | null =>
        Some(FileRangeReader(Paths.get(uri).toFile))

      case "http" | "https" =>
        Some(HttpRangeReader(uri.toURL()))

      case scheme =>
        None
    }
  }.map(LoggingRangeReader(_))


  def workingGeoTiffCrop[T <: CellGrid](tiff: GeoTiff[T], extent: Extent): Raster[T] = {
    val bounds = tiff.rasterExtent.gridBoundsFor(extent)
    val clipExtent = tiff.rasterExtent.extentFor(bounds)
    val clip = tiff.crop(List(bounds)).next._2
    Raster(clip, clipExtent)
  }

  def fetchCroppedTile(uri: URI, z: Int, x: Int, y: Int, band: Int = 0): Option[Png] = {
    getRangeReader(uri).flatMap { rr => 
      val tiff = GeoTiffReader.readMultiband(rr, decompress = false, streaming = true)      
      val transform = Proj4Transform(tiff.crs, WebMercator)
      val inverseTransform = Proj4Transform(WebMercator, tiff.crs)
      val tmsTileExtent: Extent = TmsLevels(z).mapTransform.keyToExtent(x, y)
      val tmsTileRE = RasterExtent(tmsTileExtent, 256, 256)
      val tiffTileRE = ReprojectRasterExtent(tmsTileRE, inverseTransform)
      println("tmsTileRE: ", tmsTileRE.cellSize,  tmsTileRE.rows, tmsTileRE.cols)
      println("tiffTileRE: ", tiffTileRE.cellSize, tiffTileRE.rows, tiffTileRE.cols)
      
      if (tiffTileRE.extent.intersects(tiff.extent)) {
        val overview = closestTiffOverview(tiff, tiffTileRE.cellSize, Auto(0))
        println(s"Selected overview: ${overview.tile.cols}x${overview.tile.rows}");
        // GT BUG: this crop will produce incorret raster
        //val raster = overview.crop(tiffTileRE.extent).raster
        val raster = workingGeoTiffCrop(overview, tiffTileRE.extent)
        println(s"Raster: ${raster.extent} ${raster.cols}x${raster.rows}")
        val hist = raster.tile.bands(band).histogramDouble
        val png = raster.reproject(tmsTileRE, transform, inverseTransform).tile.band(band).renderPng(Viridis.toColorMap(hist))
        Some(png)
      } else None
    }
  }
}
