package io.velo.dyn;

import groovy.lang.GroovyClassLoader;
import io.velo.Utils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RefreshLoader {
    public static RefreshLoader create(GroovyClassLoader gcl) {
        return new RefreshLoader(gcl);
    }

    private static final Map<String, Long> scriptTextLastModified = new HashMap<>();
    private static final Map<String, String> scriptTextCached = new HashMap<>();

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

    private final List<String> dirList = new ArrayList<>();

    private final GroovyClassLoader gcl;

    private RefreshLoader(GroovyClassLoader gcl) {
        this.gcl = gcl;
    }

    public RefreshLoader addDir(String dir) {
        dirList.add(dir);
        return this;
    }

    public void refresh() {
        for (var dir : dirList) {
            var d = new File(dir);
            if (!d.exists() || !d.isDirectory()) {
                continue;
            }

            // recursively refresh all groovy files
            FileUtils.listFiles(d, new String[]{CachedGroovyClassLoader.GROOVY_FILE_EXT.substring(1)}, true)
                    .forEach(this::refreshFile);
        }
    }

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RefreshLoader.class);

    private final Map<String, Long> lastModified = new HashMap<>();

    public void refreshFile(File file) {
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

    private Consumer<File> refreshFileCallback;

    RefreshLoader refreshFileCallback(Consumer<File> refreshFileCallback) {
        this.refreshFileCallback = refreshFileCallback;
        return this;
    }
}
