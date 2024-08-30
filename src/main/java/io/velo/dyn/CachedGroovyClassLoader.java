package io.velo.dyn;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.Script;
import groovy.transform.CompileStatic;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.slf4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CachedGroovyClassLoader {
    // singleton
    private CachedGroovyClassLoader() {
    }

    private static final CachedGroovyClassLoader instance = new CachedGroovyClassLoader();

    public static CachedGroovyClassLoader getInstance() {
        return instance;
    }

    private final Logger log = org.slf4j.LoggerFactory.getLogger(CachedGroovyClassLoader.class);

    private volatile GroovyClassLoader gcl;

    public GroovyClassLoader getGcl() {
        return gcl;
    }

    final static String GROOVY_FILE_EXT = ".groovy";
    final static String GROOVY_FILE_ENCODING = StandardCharsets.UTF_8.name();

    public synchronized void init(ClassLoader parentClassLoader, String classpath, CompilerConfiguration config) {
        // already initialized
        if (gcl != null) {
            log.warn("already initialized");
            return;
        }

        var configUsed = config != null ? config : new CompilerConfiguration();
        // default settings
        // utf-8
        configUsed.setSourceEncoding(GROOVY_FILE_ENCODING);
        // compile static
        configUsed.addCompilationCustomizers(new ASTTransformationCustomizer(CompileStatic.class));

        gcl = new Loader(parentClassLoader != null ? parentClassLoader : CachedGroovyClassLoader.class.getClassLoader(), configUsed);
        if (classpath != null) {
            for (String path : classpath.split(":")) {
                gcl.addClasspath(path);
                log.warn("Cached groovy class loader add classpath: " + path);
            }
        }
    }

    public Object eval(String scriptText) {
        return eval(scriptText, null);
    }

    public Object eval(String scriptText, Map<String, Object> variables) {
        var clz = gcl.parseClass(scriptText);
        Script script = null;
        try {
            script = (Script) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (variables != null) {
            var b = new Binding();
            for (var entry : variables.entrySet()) {
                b.setProperty(entry.getKey(), entry.getValue());
            }
            script.setBinding(b);
        }
        return script.run();
    }

    private static class Loader extends GroovyClassLoader {
        Loader(ClassLoader loader, CompilerConfiguration config) {
            super(loader, config);
        }

        private final Logger log = org.slf4j.LoggerFactory.getLogger(Loader.class);

        private final Map<String, Long> lastModified = new HashMap<>();
        private final Map<String, Class<?>> classLoaded = new HashMap<>();

        private boolean isModified(File f) {
            var l = lastModified.get(f.getAbsolutePath());
            return l != null && l != f.lastModified();
        }

        private void logClassLoaded(Map<String, Class> x) {
            for (var entry : x.entrySet()) {
                log.debug(entry.getKey() + ':' + entry.getValue());
            }
        }

        private static boolean isFileMatchTargetClass(String filePath, String className) {
            return filePath.replace(GROOVY_FILE_EXT, "").replaceAll("/", ".").endsWith(className);
        }

        private Class<?> getFromCache(GroovyCodeSource source) {
            Class<?> r = null;
            synchronized (classCache) {
                r = classCache.entrySet().stream()
                        .filter(entry -> isFileMatchTargetClass(source.getName(), entry.getKey()))
                        .map(Map.Entry::getValue)
                        .findFirst()
                        .orElse(null);

                if (r != null) {
                    lastModified.put(source.getFile().getAbsolutePath(), source.getFile().lastModified());
                }
            }
            return r;
        }

        @Override
        public Class<?> parseClass(GroovyCodeSource codeSource, boolean shouldCacheSource) throws CompilationFailedException {
            if (log.isDebugEnabled()) {
                logClassLoaded(classCache);
            }

            Class r = null;
            var file = codeSource.getFile();
            var scriptText = codeSource.getScriptText();
            var name = codeSource.getName();

            if (file != null) {
                if (!isModified(file)) {
                    synchronized (classLoaded) {
                        r = classLoaded.get(name);
                    }
                    if (r == null) {
                        r = getFromCache(codeSource);
                    }
                }

                if (r == null) {
                    r = super.parseClass(codeSource, false);
                    classLoaded.put(name, r);
                    lastModified.put(file.getAbsolutePath(), file.lastModified());
                    log.debug("recompile - " + name);
                } else {
                    log.debug("get from cached - " + name);
                }
            } else if (scriptText != null) {
                synchronized (classLoaded) {
                    r = classLoaded.get(scriptText);
                }
                if (r == null) {
                    r = super.parseClass(codeSource, false);
                    classLoaded.put(scriptText, r);
                    log.debug("recompile - " + scriptText);
                } else {
                    log.debug("get from cached - " + scriptText);
                }
            }
            return r;
        }
    }
}
