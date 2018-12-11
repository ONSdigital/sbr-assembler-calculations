import processing.Calculations
import util.CommandLineParser

object AssemblerCalculations {

  def main(args: Array[String]): Unit = {

    CommandLineParser(args)

    Calculations.runPipeLine()

  }

}
