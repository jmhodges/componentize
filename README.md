# What is this? Is it is awesome?

Componentize is an implementation of the connected component MapReduce
algorithm found in [Graph Twiddling in a MapReduce
World](http://www2.computer.org/portal/web/csdl/doi/10.1109/MCSE.2009.120).

It is my understanding that it is pretty cool. I don't know about
"awesome", yet. We'll see.

# Requirements
* [Scala](http://www.scala-lang.org/) 2.7.6 or thereabouts
* [Buildr](http://buildr.apache.org/)
* [Hadoop](http://hadoop.apache.org/) 0.20.1

(The why and wherefore: Componentize is written in Scala, uses Hadoop
0.20.1 to distribute the work, and Buildr for building the source
code.)

# Doing the Thing

## Building the Jarfile
To run this pretty little creature, you'll need to create it's jar
file.


To do that, you'll run `buildr package` (or `jbuildr package` if you are on OS X and
have to use the JRuby version). However! However, you will need in
your `CLASSPATH` the Hadoop 0.20.1 core jar, all of the Hadoop's
depedencies (found in the `lib` directory of the 0.20.1 tarball,
natch), and, of course, the Scala jarfiles.

A couple of nice things: 

1. Buildr will go out of its way to find the
Scala jarfiles and add them for you. I know for a fact, in fact, that
if you install Scala through MacPorts on OS X, the `jbuildr` command
will find your Scala jarfiles and add them for you to the classpath
Componentize will be built with.

2. If you create a `lib` directory in your
checkout of Componentize and copy all of the Hadoop and Hadoop
dependency jarfiles to it, you will magically have them added to
classpath used to build Componentize.

In any case, the jarfile will appear in the `target` directory as
`componentize-$someversion.jar`. 

## Running the Hadoop job

The jarfile created in the `target` directory by `buildr package` can
then be added to your `HADOOP_CLASSPATH`, or just run with the usual
single-core, single-machine Hadoop command like so:

    $HADOOP_HOME/bin/hadoop jar target/componentize-1.0.0.jar \
      componentize.Main -edgedir edges/

### Input

Before you do that, you'll need some input data! In the `edgedir` I've
slyly mentioned in the above command, you'll need one or more files
with data formatted as:

     nodeid1	nodeid2
     nodeid56	nodeid1000

and so on. Let's be clear here, those are big fat TAB characters in
the middle there. TAB as in "\t"; as in that key over "Caps Lock". They
are **not** spaces.

You can find some example input data in the `examples` directory.

### Output

The output of Componentize is ... verbose. And I don't just mean what's
printed to `/dev/stdout`. In the output directory, you'll have 6
directories of output for each iteration of Componentize. This is not
a lot of fun.

However, when Componentize finishes it will be a good sport and log
what directory you can find your output in at the end. This directory
will be a directory starting with `zonefiles` and ending in a
number. It will also be the `zonefiles` directory with the greatest
number at the end.

This output directory will be filled with one or more tab and newline
delimited files where the first column is the component label (or
"zone") of the node in the second. As in:

    zone1	nodeid73
    zone2	nodeid100

# Things That Would Be Nice

* We should use `Combiners` to reduce our load.

* There is probably a better way of storing the transient data between
  jobs than just simply writing out another output directory with
  `SequenceFiles` in it.

* There is no way to do only *some* of the processing. It is all of
  the set up and phases or none at all. Though, we could just make it
  easier for someone to configure the `Phases` themselves.

# Bonus

On build, we use [Scala X-Ray](http://github.com/harrah/browse) to
build a nice HTML version of the code. Check it out in
`target/classes.sxr/index.html`.
