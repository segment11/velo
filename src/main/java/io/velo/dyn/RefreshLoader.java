package io.velo.dyn;

import groovy.lang.GroovyClassLoader;
import io.velo.Utils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Dynamically loads and refreshes Groovy scripts.
 * Monitors directories for changes and reloads modified Groovy files.
 */
public class RefreshLoader {

    /**
     * @param gcl the GroovyClassLoader to use for loading scripts
     * @return a new RefreshLoader instance
     */
    public static RefreshLoader create(GroovyClassLoader gcl) {
        return new RefreshLoader(gcl);
    }

    /**
     * A map to store the last modified timestamp of script files.
     */
    private static final Map<String, Long> scriptTextLastModified = new ConcurrentHashMap<>();

    /**
     * A map to cache the content of script files.
     */
    private static final Map<String, String> scriptTextCached = new ConcurrentHashMap<>();

    /**
     * @param relativeFilePath the relative file path of the Groovy script
     * @return the content of the Groovy script file
     */
    public static String getScriptText(String relativeFilePath) {
        var file = new File(Utils.projectPath(relativeFilePath));
        var lastModified = scriptTextLastModified.get(relativeFilePath);

        if (lastModified != null && lastModified == file.lastModified()) {
            return scriptTextCached.get(relativeFilePath);
        }

        String scriptText;
        try {
            scriptText = FileUtils.readFileToString(file, CachedGroovyClassLoader.GROOVY_FILE_ENCODING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        scriptTextLastModified.put(relativeFilePath, file.lastModified());
        scriptTextCached.put(relativeFilePath, scriptText);

        return scriptText;
    }

    /**
     * A list of directories to monitor for script changes.
     */
    private final List<String> dirList = new ArrayList<>();

    private final GroovyClassLoader gcl;

    private RefreshLoader(GroovyClassLoader gcl) {
        this.gcl = gcl;
    }

    /**
     * @param dir the directory path to add
     * @return this RefreshLoader instance for method chaining
     */
    public RefreshLoader addDir(String dir) {
        dirList.add(dir);
        return this;
    }

    /**
     * Recursively refreshes all Groovy files in the added directories that have been modified since the last refresh.
     */
    public void refresh() {
        for (var dir : dirList) {
            var d = new File(dir);
            if (!d.exists() || !d.isDirectory()) {
                continue;
            }

            // Recursively refresh all Groovy files in the directory
            FileUtils.listFiles(d, new String[]{CachedGroovyClassLoader.GROOVY_FILE_EXT.substring(1)}, true)
                    .forEach(this::refreshFile);
        }
    }

    /**
     * The logger used for logging refresh and error information.
     */
    private static final Logger log = LoggerFactory.getLogger(RefreshLoader.class);

    /**
     * A map to store the last modified timestamp of individual script files.
     */
    private final Map<String, Long> lastModified = new ConcurrentHashMap<>();

    private void refreshFile(File file) {
        var l = lastModified.get(file.getAbsolutePath());
        if (l != null && l == file.lastModified()) {
            return;
        }

        var name = file.getName();
        log.info("begin refresh {}", name);
        try {
            gcl.parseClass(file);
            lastModified.put(file.getAbsolutePath(), file.lastModified());
            log.info("done refresh {}", name);
            if (refreshFileCallback != null) {
                refreshFileCallback.accept(file);
            }
        } catch (Exception e) {
            log.error("fail eval - {}", name, e);
        }
    }

    /**
     * A callback function to be executed after a script file has been refreshed.
     */
    private Consumer<File> refreshFileCallback;

    /**
     * @param refreshFileCallback the callback function
     * @return this RefreshLoader instance for method chaining
     */
    RefreshLoader refreshFileCallback(Consumer<File> refreshFileCallback) {
        this.refreshFileCallback = refreshFileCallback;
        return this;
    }
}