package com.gentorox.test;

import com.gentorox.services.knowledgebase.KnowledgeBaseServiceImpl;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Loads optional .env.local as early as possible in the JUnit Platform lifecycle so that
 * discovery-time conditions like @EnabledIfEnvironmentVariable can see the values.
 *
 * Search order for the env file:
 * 1) ./.env.local (current working directory when tests run)
 * 2) ./src/onemcp/.env.local (when running from repo root)
 *
 * If the file does not exist, nothing happens (tests continue normally).
 */
public class EnvLoaderLauncherSessionListener implements LauncherSessionListener {
  private static final Logger logger = LoggerFactory.getLogger(EnvLoaderLauncherSessionListener.class);
    private static volatile boolean loaded = false;

    @Override
    public void launcherSessionOpened(LauncherSession session) {
        if (loaded) return;
        loaded = true;
        loadOptionalEnvFile();
    }

    private void loadOptionalEnvFile() {
        Path path = findEnvFile();
        if (path == null) return;
        Map<String, String> vars = readKeyValueFile(path);
        if (vars.isEmpty()) return;

        // Set as System properties (non-destructive: don't override if already set)
        for (Map.Entry<String, String> e : vars.entrySet()) {
            if (isBlank(System.getProperty(e.getKey()))) {
                System.setProperty(e.getKey(), e.getValue());
            }
        }

        // Best-effort injection into process env so @EnabledIfEnvironmentVariable can see them.
        try {
            setEnvIfMissing(vars);
        } catch (Throwable ignored) {
          logger.warn("Failed to mutate env", ignored);
        }
    }

    private Path findEnvFile() {
        // 1) CWD
        Path p1 = Paths.get(".env.local");
        if (Files.exists(p1)) return p1;
        // 2) Module-relative when running from repo root
        Path p2 = Paths.get("src/onemcp/.env.local");
        if (Files.exists(p2)) return p2;
        return null;
    }

    private boolean isBlank(String s) { return s == null || s.trim().isEmpty(); }

    private Map<String, String> readKeyValueFile(Path path) {
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return br.lines()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .filter(line -> !line.startsWith("#"))
                    .map(this::parseLine)
                    .filter(e -> e.getKey() != null && !e.getKey().isEmpty())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b));
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    private Map.Entry<String, String> parseLine(String line) {
        int idx = line.indexOf('=');
        if (idx <= 0) return Map.entry("", "");
        String key = line.substring(0, idx).trim();
        String rawVal = line.substring(idx + 1).trim();
        String val = rawVal;
        if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
            val = val.substring(1, val.length() - 1);
        }
        return Map.entry(key, val);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void setEnvIfMissing(Map<String, String> newEnv) throws Exception {
        Map<String, String> toSet = new HashMap<>();
        for (Map.Entry<String, String> e : newEnv.entrySet()) {
            String curr = System.getenv(e.getKey());
            if (curr == null || curr.isEmpty()) {
                toSet.put(e.getKey(), e.getValue());
            }
        }
        if (toSet.isEmpty()) return;

        try {
            Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
            java.lang.reflect.Field theEnvironmentField = pe.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(toSet);
            try {
                java.lang.reflect.Field cienv = pe.getDeclaredField("theCaseInsensitiveEnvironment");
                cienv.setAccessible(true);
                Map<String, String> cienvMap = (Map<String, String>) cienv.get(null);
                cienvMap.putAll(toSet);
            } catch (NoSuchFieldException ignore) { }
            return;
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException ignored) {
            // fallthrough
        }

        try {
            Map<String, String> env = System.getenv();
            Class<?> unmodifiableMapClass = Class.forName("java.util.Collections$UnmodifiableMap");
            java.lang.reflect.Field mField = unmodifiableMapClass.getDeclaredField("m");
            mField.setAccessible(true);
            Object obj = mField.get(env);
            Map map = (Map) obj;
            map.putAll(toSet);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw e;
        }
    }
}
