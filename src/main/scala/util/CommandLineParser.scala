package util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, OptionGroup, Options, ParseException}
import pipeline.BuildInfo

/**
  * Parse the command line arguments and add them to system properties.
  *
  * This must be called before the Config object is created so it can be
  * merged with the properties file items or override them as required.
  *
  */
object CommandLineParser {

  val help: Option = Option.builder("h")
    .required(false)
    .hasArg(false)
    .longOpt("help")
    .desc("Print this message.")
    .build

  val version: Option = Option.builder("v")
    .required(false)
    .hasArg(false)
    .longOpt("version")
    .desc("Show version information and exit")
    .build

  val debug: Option = Option.builder("d")
    .required(false)
    .hasArg(false)
    .longOpt("debug")
    .desc("Switch debugging on")
    .build

  val enterpriseTableName: Option = Option.builder("etn")
    .required(false)
    .hasArg(true)
    .argName("TABLE NAME")
    .longOpt("enterprise-table-name")
    .desc("HBase Enterprise table name")
    .build

  val enterpriseNamespace: Option = Option.builder("ens")
    .required(false)
    .hasArg(true)
    .argName("NAMESPACE")
    .longOpt("enterprise-table-namespace")
    .desc("Enterprise table namespace")
    .build

  val enterpriseColumnFamily: Option = Option.builder("ecf")
    .required(false)
    .hasArg(true)
    .argName("COLUMN FAMILY")
    .longOpt("enterprise-column-family")
    .desc("Enterprise column family")
    .build

  val enterpriseHfilePath: Option = Option.builder("efp")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("enterprise-file-path")
    .desc("Enterprise file path")
    .build

  val linksTableName: Option = Option.builder("ltn")
    .required(false)
    .hasArg(true)
    .argName("TABLE NAME")
    .longOpt("links-table-name")
    .desc("HBase Links table name")
    .build

  val linksNamespace: Option = Option.builder("lns")
    .required(false)
    .hasArg(true)
    .argName("NAMESPACE")
    .longOpt("links-table-namespace")
    .desc("Links table namespace")
    .build

  val linksColumnFamily: Option = Option.builder("lcf")
    .required(false)
    .hasArg(true)
    .argName("COLUMN FAMILY")
    .longOpt("links-column-family")
    .desc("Links column family")
    .build

  val linksHfilePath: Option = Option.builder("lfp")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("links-file-path")
    .desc("Links file path")
    .build

  val legalTableName: Option = Option.builder("letn")
    .required(false)
    .hasArg(true)
    .argName("TABLE NAME")
    .longOpt("legal-table-name")
    .desc("HBase Legal table name")
    .build

  val legalNamespace: Option = Option.builder("lens")
    .required(false)
    .hasArg(true)
    .argName("NAMESPACE")
    .longOpt("legal-table-namespace")
    .desc("Legal table namespace")
    .build

  val legalColumnFamily: Option = Option.builder("lecf")
    .required(false)
    .hasArg(true)
    .argName("COLUMN FAMILY")
    .longOpt("legal-column-family")
    .desc("Legal column family")
    .build

  val legalHfilePath: Option = Option.builder("lefp")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("legal-file-path")
    .desc("Legal file path")
    .build

  val localTableName: Option = Option.builder("lotn")
    .required(false)
    .hasArg(true)
    .argName("TABLE NAME")
    .longOpt("local-table-name")
    .desc("HBase Local table name")
    .build

  val localNamespace: Option = Option.builder("lons")
    .required(false)
    .hasArg(true)
    .argName("NAMESPACE")
    .longOpt("local-table-namespace")
    .desc("Local table namespace")
    .build

  val localColumnFamily: Option = Option.builder("locf")
    .required(false)
    .hasArg(true)
    .argName("COLUMN FAMILY")
    .longOpt("local-column-family")
    .desc("Local column family")
    .build

  val localHfilePath: Option = Option.builder("lofp")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("local-file-path")
    .desc("Local file path")
    .build

  val reportingTableName: Option = Option.builder("retn")
    .required(false)
    .hasArg(true)
    .argName("TABLE NAME")
    .longOpt("reporting-table-name")
    .desc("HBase Reporting table name")
    .build

  val reportingNamespace: Option = Option.builder("rens")
    .required(false)
    .hasArg(true)
    .argName("NAMESPACE")
    .longOpt("reporting-table-namespace")
    .desc("Reporting table namespace")
    .build

  val reportingColumnFamily: Option = Option.builder("recf")
    .required(false)
    .hasArg(true)
    .argName("COLUMN FAMILY")
    .longOpt("reporting-column-family")
    .desc("Reporting column family")
    .build

  val reportingHfilePath: Option = Option.builder("refp")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("reporting-file-path")
    .desc("Reporting file path")
    .build

  val timePeriod: Option = Option.builder("tp")
    .required(false)
    .hasArg(true)
    .argName("TIME PERIOD")
    .longOpt("time-period")
    .desc("Time Period")
    .build

  val payeFilePath: Option = Option.builder("paye")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("paye-file-path")
    .desc("PAYE file path")
    .build

  val vatFilePath: Option = Option.builder("vat")
    .required(false)
    .hasArg(true)
    .argName("FILE PATH")
    .longOpt("vat-file-path")
    .desc("VAT file path")
    .build

  val options = new Options

  options.addOption(help)
  options.addOption(debug)

  options.addOption(enterpriseTableName)
  options.addOption(enterpriseNamespace)
  options.addOption(enterpriseColumnFamily)
  options.addOption(enterpriseHfilePath)

  options.addOption(linksTableName)
  options.addOption(linksNamespace)
  options.addOption(linksColumnFamily)
  options.addOption(linksHfilePath)

  options.addOption(legalTableName)
  options.addOption(legalNamespace)
  options.addOption(legalColumnFamily)
  options.addOption(legalHfilePath)

  options.addOption(localTableName)
  options.addOption(localNamespace)
  options.addOption(localColumnFamily)
  options.addOption(localHfilePath)

  options.addOption(reportingTableName)
  options.addOption(reportingNamespace)
  options.addOption(reportingColumnFamily)
  options.addOption(reportingHfilePath)

  options.addOption(timePeriod)

  options.addOption(payeFilePath)
  options.addOption(vatFilePath)

  val optionGroup = new OptionGroup
  optionGroup.addOption(version)
  options.addOptionGroup(optionGroup)

  def apply(args: Array[String]): Unit = {

    val parser = new DefaultParser

    try {

      val line: CommandLine = parser.parse(options, args)

      if (line.hasOption(help.getOpt)) {
        val formatter = new HelpFormatter
        formatter.setWidth(100)
        val header = "\n(Re)Run the SBR Enterprise Calculations\n\n"

        formatter.printHelp(s"${BuildInfo.name}", header, options, null, true)
        System.exit(0)
      }

      if (line.hasOption(version.getOpt)) {
        println(s"${BuildInfo.name}")
        val compiled = new Date(BuildInfo.buildTime)
        val format = new SimpleDateFormat("dd MMMM yyyy 'at' HH:mm:ss a").format(compiled)
        println(s"Version ${BuildInfo.version} built on $format")
        System.exit(0)
      }

      if (line.hasOption(debug.getOpt)) {
        System.setProperty("debug", "true")
      }

    } catch {
      case exp: ParseException =>
        // oops, something went wrong
        println(s"Options Error: ${exp.getMessage}")
        System.exit(1)
    }
  }
}
