require 'buildr/scala'

VERSION_NUMBER = "1.0.0"
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

  compile.dependencies << FileList['lib/*.jar']

  package(:jar)
end
