package frontend.gui

import org.apache.batik.dom.GenericDOMImplementation
import org.apache.batik.svggen._

import java.io.{FileOutputStream, OutputStream, PrintStream}
import scala.swing.{Component, Dimension}
object SVGGenerator {
  val domImpl = GenericDOMImplementation.getDOMImplementation

  // Create an instance of org.w3c.dom.Document.
  val svgNS = "http://www.w3.org/2000/svg"
  val document = domImpl.createDocument(svgNS, "svg", null)

  // Create an instance of the SVG Generator.
  val svgGenerator = new SVGGraphics2D(document)

  def save(c: Component, name: String): Unit = {
    svgGenerator.setSVGCanvasSize(c.peer.getSize())
    c.paint(svgGenerator)
    val useCSS = true // we want to use CSS style attributes
    svgGenerator.stream(name, useCSS)
  }
}
