package componentize

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import org.apache.hadoop.fs.Path

import java.lang.{Iterable => JavaItb}
import java.util.{Iterator => JavaItr}
import java.io.File

trait HadoopInterop {
  implicit def conf2ComponentConf(conf: Configuration) = new {
    def edgePath : Path = {
      new Path(new File(conf.get("componentize.edgedir")).getCanonicalPath)
    }

    def outputPath(dirname: String) : Path = {
      outputPath(dirname, rotation)
    }

    def outputPath(dirname: String, rot: Long) = {
      val outdir = new File(conf.get("componentize.outputdir")).getCanonicalPath
      new Path(outdir + File.separatorChar + dirname + rot)
    }

    def rotation : Long = { conf.getLong("componentize.rotation", 0) }

    def hasZoneTransfers : Boolean = {
      conf.getBoolean("componentize.hasnewzonetransfers", false)
    }
  }

  implicit def string2Text(str: String) : Text = new Text(str)



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
}
