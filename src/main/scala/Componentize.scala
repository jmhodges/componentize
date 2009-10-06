package componentize

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
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
import java.io.File

import java.lang.{Iterable => JavaItb}
import java.util.{Iterator => JavaItr}

object Main {
  def main(args: Array[String]) : Unit = {
    val result = ToolRunner.run(new Configuration(), new Componentize(), args);
    System.exit(result)
  }
}

trait ConfigImplicits {
  implicit def conf2ComponentConf(conf: Configuration) = new {
    def edgePath : Path = {
      new Path(new File(conf.get("componentize.edgedir")).getCanonicalPath)
    }

    def outputPath(dirname: String) : Path = {
      outputPath(dirname, rotation)
    }

    def outputPath(dirname: String, rot: Long) = {
      val foo = new File(conf.get("componentize.outputdir")).getCanonicalPath
      new Path(foo + File.separatorChar + dirname + rot)
    }

    def rotation : Long = { conf.getLong("componentize.rotation", 0) }
  }
}

object Componentize extends ConfigImplicits {
  type Conf = Configuration

  implicit def string2Text(str: String) : Text = new Text(str)

  val LOG = Logger.getRootLogger()

  class UnjackedIterable[T](private val jtb: JavaItb[T]) extends Iterable[T] {
    def elements: Iterator[T] = jtb.iterator
  }
  
  class UnjackedIterator[T](private val jit: JavaItr[T]) extends Iterator[T] {
    def hasNext: Boolean = jit.hasNext
    
    def next: T = jit.next
  }

  implicit def jitb2sitb[T](jtb: JavaItb[T]): Iterable[T] = new UnjackedIterable(jtb)
  implicit def jitr2sitr[T](jit: JavaItr[T]): Iterator[T] = new UnjackedIterator(jit)

  class SMapper[A,B,C,D] extends Mapper[A,B,C,D] {
    type Context = Mapper[A,B,C,D]#Context
  }

  class SReducer[A,B,C,D] extends Reducer[A,B,C,D] {
    type Context = Reducer[A,B,C,D]#Context

    override def reduce(key: A, values: JavaItb[B], context: Context) {
      reduce(key, jitb2sitb(values), context)
    }

    // This prettys up our code by letting us use a real iterable
    // instead of Java's terrible one.
    def reduce(key: A, values: Iterable[B], context: Context) {
      for (value <- values) {
        context.write(key.asInstanceOf[C], value.asInstanceOf[D])
      }
    }
  }

  // Because hadoop requires it and type erasure is awful.
  class TextArrayWritable(klass: java.lang.Class[_ <: Writable])
                                           extends ArrayWritable(klass) {
    // This should really use UTF8, but we're already on this train,
    // so let's ride it.
    def this() = this(classOf[Text])
    def this(strings: Array[String]) = {
      this(classOf[Text])
      set(strings.map(new Text(_)))
    }
    def this(texts: Array[Text]) = {
      this(classOf[Text])
      set(texts)
    }

    def set(texts: Array[Text]) : Unit = {
      set(texts.asInstanceOf[Array[Writable]])
    }

    // this is for people who like to debug with TextOutputFormat
    override def toString() : String = toStrings.mkString(",")

  }

  // For Iterable.min(Iterable[Text])
  class RichText(protected val txt: Text) extends Ordered[Text] {
    def compare(that: Text) = txt.compareTo(that)
    def compare(that: RichText) = txt.compareTo(that.txt)
  }

  implicit def text2RichText(txt: Text) : RichText = new RichText(txt)

  object SetUp {
    class ZoneFileFromEdgeFileMapper extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context:Context) = {
        val pair = line.toString.split("\t")
        pair.foreach(node => context.write(node,node))
      }
    }

    class ZoneFileFromEdgeFileReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(node: Text, sameNodes: Iterable[Text], context:Context) {
        context.write(node, node)
      }
    }

    def run(conf: Conf) : Boolean = {
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
    val FromZoneFile = new Text("zonednode")
    val FromEdgeFile = new Text("edgednode")

    class EdgeMapper extends SMapper[LongWritable, Text, Text, TextArrayWritable] {
      // The key emitted is the zone (a.k.a. the key in EdgeZoneJoin)
      override def map(key: LongWritable, line: Text, context:Context)  = {
        val pair = line.toString.split("\t")
        val aw = new TextArrayWritable(Array(FromEdgeFile,
                                             new Text(pair.mkString(","))))
        pair.foreach(node => context.write(new Text(node), aw))
      }
    }

    class ZoneMapper extends SMapper[LongWritable, Text, Text, TextArrayWritable] {
      override def map(key: LongWritable, line: Text, context: Context) = {
        val zoneAndNode = line.toString.split("\t")
        var zone = zoneAndNode(0)
        val node = zoneAndNode(1)
        val aw = new TextArrayWritable(Array(FromZoneFile, new Text(zone)))

        context.write(new Text(node), aw)
      }
    }
    
    class EdgeZoneJoinMapper
    extends SMapper[Text, TextArrayWritable, Text, TextArrayWritable] {}
    
    // Outputs EdgeWithOneZoneFile
    class EdgeZoneJoinReducer
    extends SReducer[Text, TextArrayWritable, Text, Text] {
      override def reduce(node: Text,
                          zoneAndEdges: Iterable[TextArrayWritable],
                          context: Context) = {
        var zones = List[Text]()
        var edges = List[Text]()
        for (aw <- zoneAndEdges) {
          var arr = aw.get
          if(arr(0) == FromEdgeFile) {
            edges = arr(1).asInstanceOf[Text] :: edges
          } else {
            zones = arr(1).asInstanceOf[Text] :: zones
          }
        }
        
        for(edge <- edges) {
          context.write(edge, zones(0))
        }
      }
    }
    
    def run(conf: Conf) : Boolean = {
      LOG.info("Running FirstPhase.")
      
      val edgeJob = mapperJob(conf, classOf[EdgeMapper],
                              "edge mapper",
                              conf.edgePath,
                              conf.outputPath("edgefilesjoin"))
      if (!edgeJob.waitForCompletion(true)) return false

      val zoneJob = mapperJob(conf,classOf[ZoneMapper],
                              "zone mapper",
                              conf.outputPath("zonefiles", conf.rotation-1),
                              conf.outputPath("zonefilesjoin"))
      if (!zoneJob.waitForCompletion(true)) return false

      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def mapperJob(conf: Conf,
                  mapperKlass: Class[_ <: Mapper[_,_,_,_]],
                  name: String,
                  inputPath: Path,
                  outputPath: Path) : Job = {
      val job = new Job(conf, name)
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(mapperKlass)
      
      job.setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(job, inputPath)
      FileOutputFormat.setOutputPath(job, outputPath)
      
      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[TextArrayWritable])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[TextArrayWritable])
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]])

      return job
    }

    def joinJob(conf: Conf) : Job = {
      val job = new Job(conf, "edge zone join")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[EdgeZoneJoinMapper])
      job.setReducerClass(classOf[EdgeZoneJoinReducer])

      job.setInputFormatClass(
        classOf[SequenceFileInputFormat[Text, TextArrayWritable]])
      FileInputFormat.setInputPaths(job,
                                    conf.outputPath("edgefilesjoin"),
                                    conf.outputPath("zonefilesjoin"))
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])

      FileOutputFormat.setOutputPath(job, conf.outputPath("edgewithonezonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[TextArrayWritable])
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
            context.write(new Text(zone), new Text(smallestZone))
          }
        }
      }
    }

    def run(conf: Conf) : Boolean = {
      val job = new Job(conf, "interzone")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[InterZoneMapper])
      job.setReducerClass(classOf[InterZoneReducer])
      
      job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text]])
      FileInputFormat.setInputPaths(job, conf.outputPath("edgewithonezonefiles"))

      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.outputPath("interzonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      LOG.info("Running SecondPhase.")

      val boo = job.waitForCompletion(true)
      val zoneTrans = job.getCounters.findCounter("componentize", "zonetrans").getValue
      conf.setBoolean("componentize.hasnewzonetransfers", zoneTrans != 0)

      boo
    }
  }

  object ThirdPhase {
    val FromInterZone = new Text("interzone")
    val FromZoneFile = new Text("zonednode")

    class InterZoneMapper extends SMapper[Text, Text, Text, TextArrayWritable] {
      override def map(oldZone: Text, newZone: Text, context: Context) = {
        val aw = new TextArrayWritable(Array(FromInterZone, newZone))
        context.write(oldZone, aw)
      }
    }

    class ZoneFileVertexMapper
    extends SMapper[LongWritable, Text, Text, TextArrayWritable] {
      override def map(key: LongWritable, line: Text, context: Context) = {
        val zoneAndNode = line.toString.split("\t")
        val zone = zoneAndNode(0)
        val node = zoneAndNode(1)
        val aw = new TextArrayWritable(Array(FromZoneFile, new Text(node)))
        
        context.write(new Text(zone), aw)
      }
    }

    class ZoneFileMapper
    extends SMapper[Text, TextArrayWritable, Text, TextArrayWritable] {}
    
    class ZoneFileReducer
    extends SReducer[Text, TextArrayWritable, Text, Text] {
      override def reduce(oldZone: Text,
                          nodesAndNewZones: Iterable[TextArrayWritable],
                          context: Context) = {
        var smallestZone = oldZone.toString
        var possible = smallestZone
        var nodes = List[String]()
        
        for (nNZ <- nodesAndNewZones) {
          var arr = nNZ.get
          if (arr(0) == FromInterZone) {
            possible = arr(1).asInstanceOf[Text].toString
            if (smallestZone > possible) smallestZone = possible

          } else {
            nodes = arr(1).asInstanceOf[Text].toString :: nodes
          }
        }

        nodes.foreach(node =>
          context.write(new Text(smallestZone), new Text(node)))
      }
    }

    def run(conf: Conf) : Boolean = {
      Componentize.LOG.info("Running ThirdPhase.")
      val interzoneJob = mapperJob(conf,
                                   classOf[InterZoneMapper],
                                   "interzone mapper",
                                   conf.outputPath("interzonefiles"),
                                   conf.outputPath("interzonefilestransitionsjoin"),
                                   classOf[SequenceFileInputFormat[Text, Text]]
                                 )

      if (!interzoneJob.waitForCompletion(true)) return false

      Componentize.LOG.info("Finished interzone mapper.")
      val zoneVertexJob = mapperJob(conf,
                                    classOf[ZoneFileVertexMapper],
                                    "zone vertex mapper",
                                    conf.outputPath("zonefiles", conf.rotation-1),
                                    conf.outputPath("zonefilestransitionsjoin"),
                                    classOf[TextInputFormat]
                                  )

      if (!zoneVertexJob.waitForCompletion(true)) return false
      Componentize.LOG.info("Finished zone vertex mapper")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def mapperJob(conf: Conf,
                  mapperKlass: Class[_ <: Mapper[_,_,_,_]],
                  name: String,
                  inputPath: Path,
                  outputPath: Path,
                  inputFormatKlass: Class[_ <: FileInputFormat[_,_]]) : Job = {
      val job = new Job(conf, name)
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(mapperKlass)
      job.setInputFormatClass(inputFormatKlass)

      FileInputFormat.setInputPaths(job, inputPath)
      job.setOutputFormatClass(
        classOf[SequenceFileOutputFormat[Text, TextArrayWritable]])
      FileOutputFormat.setOutputPath(job, outputPath)

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[TextArrayWritable])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[TextArrayWritable])
      return job
    }

    def joinJob(conf: Conf) : Job = {
      val job = new Job(conf, "edge zone join")
      job.setJarByClass(classOf[Componentize])
      job.setMapperClass(classOf[ZoneFileMapper])
      job.setReducerClass(classOf[ZoneFileReducer])

      job.setInputFormatClass(
        classOf[SequenceFileInputFormat[Text, TextArrayWritable]])
      FileInputFormat.setInputPaths(job,
                                    conf.outputPath("interzonefilestransitionsjoin"),
                                    conf.outputPath("zonefilestransitionsjoin"))

      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.outputPath("zonefiles"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[TextArrayWritable])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      return job
    }
  }
}

class Componentize extends Configured with Tool with ConfigImplicits {
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
    } while (hasNewZoneTransfers(conf))

    Componentize.LOG.info("Output directory: "+conf.outputPath("zonefiles"))
    return 0
  }

  def hasNewZoneTransfers(conf: Configuration) : Boolean = {
    conf.getBoolean("componentize.hasnewzonetransfers", false)
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
