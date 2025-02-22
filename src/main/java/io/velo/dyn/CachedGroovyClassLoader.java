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
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * A singleton class that provides a cached Groovy class loader.
 * This class ensures that Groovy scripts are only compiled once and cached for subsequent executions.
 */
public class CachedGroovyClassLoader {
    // Singleton instance
    private CachedGroovyClassLoader() {
    }

    private static final CachedGroovyClassLoader instance = new CachedGroovyClassLoader();

    /**
     * Returns the singleton instance of CachedGroovyClassLoader.
     *
     * @return the singleton instance of CachedGroovyClassLoader
     */
    public static CachedGroovyClassLoader getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(CachedGroovyClassLoader.class);

    private volatile GroovyClassLoader gcl;

    /**
     * Returns the GroovyClassLoader instance.
     *
     * @return the GroovyClassLoader instance
     */
    public GroovyClassLoader getGcl() {
        return gcl;
    }

    final static String GROOVY_FILE_EXT = ".groovy";
    final static String GROOVY_FILE_ENCODING = StandardCharsets.UTF_8.name();

    /**
     * Initializes the GroovyClassLoader with the given class loader, classpath, and compiler configuration.
     *
     * @param parentClassLoader the parent ClassLoader to use; if null, the class loader of CachedGroovyClassLoader is used
     * @param classpath         a string containing paths to add to the classpath, separated by colons
     * @param config            the CompilerConfiguration to use; if null, a default configuration is used
     */
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
                log.warn("Cached groovy class loader add classpath={}", path);
            }
        }
    }

    /**
     * Evaluates a Groovy script with the given script text.
     *
     * @param scriptText the Groovy script text to evaluate
     * @return the result of the script execution
     */
    public Object eval(String scriptText) {
        return eval(scriptText, null);
    }

    /**
     * Evaluates a Groovy script with the given script text and variables.
     *
     * @param scriptText the Groovy script text to evaluate
     * @param variables  a map of variables to pass to the script
     * @return the result of the script execution
     */
    public Object eval(String scriptText, Map<String, Object> variables) {
        var clz = gcl.parseClass(scriptText);
        Script script;
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

    /**
     * A custom implementation of GroovyClassLoader that caches loaded classes to improve performance.
     */
    private static class Loader extends GroovyClassLoader {
        Loader(ClassLoader loader, CompilerConfiguration config) {
            super(loader, config);
        }

        private static final Logger log = LoggerFactory.getLogger(Loader.class);

        private final Map<String, Long> lastModified = new HashMap<>();
        private final Map<String, Class<?>> classLoaded = new HashMap<>();

        /**
         * Checks if a file has been modified since it was last loaded.
         *
         * @param f the file to check
         * @return true if the file has been modified, false otherwise
         */
        private boolean isModified(File f) {
            var l = lastModified.get(f.getAbsolutePath());
            return l != null && l != f.lastModified();
        }

        /**
         * Logs the classes currently loaded in the class cache.
         *
         * @param x the map of class names to class objects
         */
        private void logClassLoaded(Map<String, Class> x) {
            for (var entry : x.entrySet()) {
                log.debug("{}:{}", entry.getKey(), entry.getValue());
            }
        }

        /**
         * Checks if a file path matches the target class name.
         *
         * @param filePath  the file path to check
         * @param className the target class name
         * @return true if the file path matches the target class name, false otherwise
         */
        private static boolean isFileMatchTargetClass(String filePath, String className) {
            return filePath.replace(GROOVY_FILE_EXT, "").replaceAll("/", ".").endsWith(className);
        }

        /**
         * Retrieves a class from the cache if it exists and is not modified.
         *
         * @param source the GroovyCodeSource for the class
         * @return the cached class if available, null otherwise
         */
        private Class<?> getFromCache(GroovyCodeSource source) {
            Class<?> r;
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

        /**
         * Parses a Groovy class from the given GroovyCodeSource, caching it if necessary.
         *
         * @param codeSource        the GroovyCodeSource containing the script text
         * @param shouldCacheSource whether to cache the source code; should always be false in this implementation
         * @return the parsed Groovy class
         * @throws CompilationFailedException if the script compilation fails
         */
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
                    log.debug("recompile - {}", name);
                } else {
                    log.debug("get from cached - {}", name);
                }
            } else if (scriptText != null) {
                synchronized (classLoaded) {
                    r = classLoaded.get(scriptText);
                }
                if (r == null) {
                    r = super.parseClass(codeSource, false);
                    classLoaded.put(scriptText, r);
                    log.debug("recompile - {}", scriptText);
                } else {
                    log.debug("get from cached - {}", scriptText);
                }
            }
            return r;
        }
    }
}