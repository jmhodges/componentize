require 'buildr/scala'

VERSION_NUMBER = "0.9.0"
# Group identifier for your projects
GROUP = "componentize"
COPYRIGHT = ""

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://www.ibiblio.org/maven2/"

desc "The Componentize project"
define "componentize" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT

  compile.with 'org.scala-tools.sxr:sxr_2.7.6:jar:0.2.3'

  compile.dependencies << FileList['lib/*.jar']

  package(:jar)

  # Scala X-Ray config
  arr = %w{org scala-tools sxr sxr_2.7.6 0.2.3 sxr_2.7.6-0.2.3.jar}
  path = File.join(*([repositories.local] +  arr))
  compile.using :other => ["-Xplugin:#{path}",
                   "-P:sxr:base-directory:#{_('src','main','scala')}"]

end
