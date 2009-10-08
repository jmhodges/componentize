package componentize

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.log4j.Logger
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import org.apache.commons.cli.{Option => CmdOption}
import org.apache.commons.cli.OptionBuilder


object Main {
  def main(args: Array[String]) : Unit = {
    val result = ToolRunner.run(new Configuration(), new Componentize(), args);
    System.exit(result)
  }
}

object Componentize extends HadoopInterop {

  val LOG = Logger.getRootLogger()

  val FromZoneFile = "0"
  val FromInterZone = "1"

  object SetUp {
    class ZoneFileFromEdgeFileMapper extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context:Context) = {
        val pair = line.toString.split("\t")
        pair.foreach(node => context.write(new Text(node), new Text(node)))
      }
    }

    class ZoneFileFromEdgeFileReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(node: Text, sameNodes: Iterable[Text], context:Context) {
        val value = new Text(node.toString + "\t" + FromZoneFile)
        context.write(node, value)
      }
    }

    def run(conf: Configuration) : Boolean = {
      val job = new Job(conf, "zonefile creation")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[ZoneFileFromEdgeFileMapper])
      job.setReducerClass(classOf[ZoneFileFromEdgeFileReducer])
      
      job.setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(job, conf.edgePath)

      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job,
                                     conf.outputPath("zonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      LOG.info("Creating the zonefiles from the original edgefiles.")
      job.waitForCompletion(true)
    }
  }

  object FirstPhase {
    class EdgeZoneJoinMapper
    extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context: Context) {
        val arr = line.toString.split("\t")

        if (arr.size < 3) {
          arr.foreach(node =>
            context.write(new Text(node), new Text(arr.mkString(","))))

        } else {
          context.write(new Text(arr(1)), new Text(arr(0)+"\t"+FromZoneFile))
        }
      }
    }
    
    // Outputs EdgeWithOneZoneFile
    class EdgeZoneJoinReducer
    extends SReducer[Text, Text, Text, Text] {
      override def reduce(node: Text,
                          zoneAndEdges: Iterable[Text],
                          context: Context) = {
        var zones = List[String]()
        var edges = List[String]()

        for (aw <- zoneAndEdges) {
          var arr = aw.toString.split("\t")
          if(arr.size < 2) {
            edges = arr(0) :: edges
          } else {
            zones = arr(0) :: zones
          }
        }
        
        for(edge <- edges) {
          context.write(new Text(edge), new Text(zones(0)))
        }
      }
    }

    def run(conf: Configuration) : Boolean = {
      LOG.info("Running FirstPhase.")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def joinJob(conf: Configuration) : Job = {
      val job = new Job(conf, "edge zone join")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[EdgeZoneJoinMapper])
      job.setReducerClass(classOf[EdgeZoneJoinReducer])

      job.setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(job,
                                    conf.edgePath,
                                    conf.outputPath("zonefiles", conf.rotation-1))
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])

      FileOutputFormat.setOutputPath(job, conf.outputPath("edgewithonezonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      return job
    }
  }

  object SecondPhase {

    class InterZoneMapper
    extends SMapper[Text, Text, Text, Text] {}

    class InterZoneReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(edge: Text,
                          zonesItb: Iterable[Text],
                          context: Context) : Unit = {
        if (zonesItb.isEmpty) return
        var zones = List[String]()

        for (z <- zonesItb) {
          zones = z.toString :: zones
        }

        val smallestZone = Iterable.min(zones)

        for (zone <- zones) {
          if(zone != smallestZone) {
            context.getCounter("componentize", "zonetrans").increment(1)
            context.write(new Text(zone), new Text(smallestZone+"\t"+FromInterZone))
          }
        }
      }
    }

    def run(conf: Configuration) : Boolean = {
      val job = new Job(conf, "interzone")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[InterZoneMapper])
      job.setReducerClass(classOf[InterZoneReducer])
      
      job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text]])
      FileInputFormat.setInputPaths(job, conf.outputPath("edgewithonezonefiles"))

      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.outputPath("interzonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      LOG.info("Running SecondPhase.")

      val res = job.waitForCompletion(true)
      val zoneTrans = job.getCounters.findCounter("componentize", "zonetrans").getValue
      conf.setBoolean("componentize.hasnewzonetransfers", zoneTrans != 0)

      res
    }
  }

  object ThirdPhase {

    class ZoneFileMapper
    extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context:Context) {
        val arr = line.toString.split("\t")
        context.write(new Text(arr(0)), new Text(arr.drop(1).mkString("\t")))
      }
    }
    
    class ZoneFileReducer
    extends SReducer[Text, Text, Text, Text] {
      override def reduce(oldZone: Text,
                          nodesAndNewZones: Iterable[Text],
                          context: Context) = {
        var smallestZone = oldZone.toString
        var possible = smallestZone
        var nodes = List[String]()
        
        for (nNZ <- nodesAndNewZones) {
          var arr = nNZ.toString.split("\t")
          if (arr(1) == FromInterZone) {
            possible = arr(0)
            if (smallestZone > possible) smallestZone = possible
          } else {
            nodes = arr(0) :: nodes
          }
        }
        val conf = context.getConfiguration()
        val tail = if (conf.hasZoneTransfers) "\t"+FromZoneFile else ""

        nodes.foreach(node =>
          context.write(new Text(smallestZone), new Text(node+tail)))
      }
    }

    def run(conf: Configuration) : Boolean = {
      Componentize.LOG.info("Running ThirdPhase.")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def joinJob(conf: Configuration) : Job = {
      val job = new Job(conf, "edge zone join")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[ZoneFileMapper])
      job.setReducerClass(classOf[ZoneFileReducer])

      job.setInputFormatClass(classOf[TextInputFormat])

      FileInputFormat.setInputPaths(job,
                                    conf.outputPath("zonefiles", conf.rotation-1),
                                    conf.outputPath("interzonefiles"))

      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.outputPath("zonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      return job
    }
  }
}

class Componentize extends Configured with Tool with HadoopInterop {
  def run(args: Array[String]): Int = {
    val conf = getRealConf(args)

    if (!Componentize.SetUp.run(conf)) {
      return 1
    }
    Componentize.LOG.info("Finished creating the zonefiles.")

    runPhases(conf)
  }

  def runPhases(conf: Configuration) : Int = {
    conf.setLong("componentize.rotation", 1)
    do {
      if (!Componentize.FirstPhase.run(conf)) {
        return 1
      }

      Componentize.LOG.info("Finished FirstPhase.")
      if (!Componentize.SecondPhase.run(conf)) {
        return 1
      }
      
      Componentize.LOG.info("Finished SecondPhase.")
      if (!Componentize.ThirdPhase.run(conf)) {
        return 1
      }
      Componentize.LOG.info("Finished ThirdPhase.")

      conf.setLong("componentize.rotation", conf.rotation+1)
    } while (conf.hasZoneTransfers)

    Componentize.LOG.info("Output directory: " +
                          conf.outputPath("zonefiles", conf.rotation-1))
    return 0
  }

  def getRealConf(args: Array[String]) : Configuration = {
    val gp = new GenericOptionsParser(getConf(), additionalOptions(), args)
    val conf = gp.getConfiguration()

    val cl = gp.getCommandLine()
    if (cl == null) {
      System.exit(1)
    }

    // Used by SecondPhase to store whether or not any new zone
    // transfers have occured. If there haven't been, we know we on
    // our final rotation of the phases.
    conf.setBoolean("componentize.hasnewzonetransfers", false)

    conf.set("componentize.edgedir",
                   cl.getOptionValue("edgedir", "edgefiles"))
    conf.set("componentize.outputdir",
                   cl.getOptionValue("outputdir", "output"))

    conf
  }

  def additionalOptions() : Options = {
    var input = new CmdOption("edgedir",
                              true,
                              "Directory to read the edgefiles from")
    input.setType("")
    var output = new CmdOption("outputdir",
                               true,
                               "Directory to write all output to")
    input.setType("")
    val ops = new Options()
    ops.addOption(input)
    ops.addOption(output)
    ops
  }
}
