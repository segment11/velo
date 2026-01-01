import com.segment.common.Conf

def c = Conf.instance
c.resetWorkDir(true)
def srcDir = c.projectPath('/src')

def targetPackagePath = '/persist/'

Map<String, Set<String>> paramComments = [:]

new File(srcDir).eachFileRecurse { f ->
    if (f.name.endsWith('.java') && f.absolutePath.contains(targetPackagePath)) {
        f.eachLine { line ->
            if (line.contains('@param')) {
                def subLine = line.substring(line.indexOf('@param'))
                def arr = subLine.split(' ')
                def paramName = arr[1]
                def comment = arr[2..-1].join(' ').trim()

                def set = paramComments.get(paramName)
                if (set == null) {
                    set = new HashSet()
                    paramComments.put(paramName, set)
                }
                set << comment
            }
            return true
        }
    }
}

paramComments.each { paramName, set ->
    if (set.size() > 1) {
        println "paramName: $paramName"
        println "set: $set"
    }
}