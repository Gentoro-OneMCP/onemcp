package com.gentorox.services.agent;

import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service responsible for resolving the Agent overall behavior from YAML configuration.
 *
 * <p>Resolution rules:
 * - Loads an internal agent.yaml from classpath (resources/agent.yaml) which contains the full schema.
 * - Optionally loads foundation/agent.yaml (or a provided foundation root) and applies overrides.
 * - If guardrails.autoGen is true, uses InferenceService to generate a cohesive set of guardrails
 *   using: the final systemPrompt, any additional guardrails content provided by overrides, and
 *   the collection of available services derived from the Knowledge Base openapi entries.
 *
 * The resulting, fully-resolved configuration can be accessed via getters or as a DTO.
 */
@Service
public class AgentService {
  private static final Logger logger = LoggerFactory.getLogger(AgentService.class);

  private final InferenceService inferenceService;
  private final KnowledgeBaseService kbService;
  // Tools are now handled by LangChain4j @Tool annotations

  private volatile AgentConfig resolvedConfig;

  public AgentService(InferenceService inferenceService, KnowledgeBaseService kbService) {
    this.inferenceService = Objects.requireNonNull(inferenceService, "inferenceService");
    this.kbService = Objects.requireNonNull(kbService, "kbService");
    // Tools are now handled by LangChain4j @Tool annotations
    // Do not auto-load here; let the host call initialize(...) once foundation is known.
  }

  /**
   * Initialize or re-initialize the Agent configuration, loading the internal defaults and applying
   * overrides from the given foundation root (expects foundation/agent.yaml at that location).
   *
   * @param foundationRoot root folder of the foundation; if null, defaults to "foundation" under CWD
   */
  public synchronized void initialize(Path foundationRoot) {
    if (foundationRoot == null) foundationRoot = Path.of("foundation");

    AgentConfig base = loadInternalConfig();
    AgentConfig overrides = loadFoundationOverrides(foundationRoot.resolve("agent.yaml"));

    AgentConfig merged = merge(base, overrides);

    // Personalize system prompt with tool placeholders before any guardrails generation
    String personalized = personalizeSystemPrompt(merged.systemPrompt());
    merged = merged.withSystemPrompt(personalized);

    // AutoGen support
    if (merged.guardrails() != null && Boolean.TRUE.equals(merged.guardrails().autoGen())) {
      String generated = generateGuardrails(merged, collectOpenApiServices());
      merged = merged.withGuardrails(merged.guardrails().withContent(generated));
    }

    this.resolvedConfig = merged;
  }

  /** Returns the final, resolved Agent configuration. Call initialize(...) first. */
  public AgentConfig getConfig() {
    AgentConfig cfg = resolvedConfig;
    if (cfg == null) throw new IllegalStateException("AgentService not initialized. Call initialize().");
    return cfg;
  }

  /** Convenience: returns the final system prompt. */
  public String systemPrompt() { return getConfig().systemPrompt(); }

  /** Convenience: returns guardrails text (after autogen if enabled). */
  public String guardrails() { return Optional.ofNullable(getConfig().guardrails()).map(AgentConfig.Guardrails::content).orElse(""); }

  // ---------- System prompt personalization ----------

  private String personalizeSystemPrompt(String systemPrompt) {
    // Tools are now handled by LangChain4j @Tool annotations, no placeholder resolution needed
    return systemPrompt;
  }

  // ---------- Loading ----------

  private AgentConfig loadInternalConfig() {
    try {
      ClassPathResource res = new ClassPathResource("agent.yaml");
      try (InputStream in = res.getInputStream()) {
        return parseYaml(in);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load internal agent.yaml from classpath", e);
    }
  }

  private AgentConfig loadFoundationOverrides(Path overridePath) {
    if (!Files.exists(overridePath)) return AgentConfig.empty();
    try (InputStream in = Files.newInputStream(overridePath)) {
      return parseYaml(in);
    } catch (Exception e) {
      logger.warn("Failed to parse foundation overrides at {}. Using defaults.", overridePath, e);
      return AgentConfig.empty();
    }
  }

  private AgentConfig parseYaml(InputStream in) {
    Yaml yaml = new Yaml(new LoaderOptions());
    Object data = yaml.load(in);
    if (!(data instanceof Map<?,?> map)) return AgentConfig.empty();
    Object agentObj = map.get("agent");
    if (!(agentObj instanceof Map<?,?> agentMap)) return AgentConfig.empty();

    // systemPrompt
    String systemPrompt = optString(agentMap.get("systemPrompt"));

    // inference block
    AgentConfig.Inference inference = null;
    Object infObj = agentMap.get("inference");
    if (infObj instanceof Map<?,?> infMap) {
      String provider = optString(infMap.get("provider"));
      String model = optString(infMap.get("model"));
      List<AgentConfig.Option> opts = null;
      Object optionsObj = infMap.get("options");
      if (optionsObj instanceof List<?> list) {
        List<AgentConfig.Option> tmp = new ArrayList<>();
        for (Object o : list) {
          if (o instanceof Map<?,?> om) {
            String name = optString(om.get("name"));
            Object value = om.get("value");
            if (name != null) tmp.add(new AgentConfig.Option(name, value));
          }
        }
        opts = tmp;
      }
      inference = new AgentConfig.Inference(provider, model, opts);
    }

    // guardrails block
    AgentConfig.Guardrails guardrails = null;
    Object grObj = agentMap.get("guardrails");
    if (grObj instanceof Map<?,?> grMap) {
      Boolean autoGen = optBoolean(grMap.get("autoGen"));
      String content = optString(grMap.get("content"));
      guardrails = new AgentConfig.Guardrails(autoGen, content);
    }

    return new AgentConfig(systemPrompt, inference, guardrails);
  }

  private static String optString(Object o) { return o == null ? null : String.valueOf(o); }
  private static Boolean optBoolean(Object o) {
    if (o instanceof Boolean b) return b;
    if (o == null) return null;
    return Boolean.parseBoolean(String.valueOf(o));
  }

  // ---------- Merge logic ----------

  private AgentConfig merge(AgentConfig base, AgentConfig overrides) {
    if (overrides == null) return base;

    String systemPrompt = firstNonNull(overrides.systemPrompt(), base.systemPrompt());

    AgentConfig.Inference infBase = base.inference();
    AgentConfig.Inference infOv = overrides.inference();
    AgentConfig.Inference inference = (infBase == null && infOv == null) ? null : new AgentConfig.Inference(
        firstNonNull(infOv != null ? infOv.provider() : null, infBase != null ? infBase.provider() : null),
        firstNonNull(infOv != null ? infOv.model() : null, infBase != null ? infBase.model() : null),
        mergeOptions(infBase, infOv)
    );

    AgentConfig.Guardrails grBase = base.guardrails();
    AgentConfig.Guardrails grOv = overrides.guardrails();
    AgentConfig.Guardrails guardrails = (grBase == null && grOv == null) ? null : new AgentConfig.Guardrails(
        firstNonNull(grOv != null ? grOv.autoGen() : null, grBase != null ? grBase.autoGen() : null),
        firstNonNull(grOv != null ? grOv.content() : null, grBase != null ? grBase.content() : null)
    );

    return new AgentConfig(systemPrompt, inference, guardrails);
  }

  private List<AgentConfig.Option> mergeOptions(AgentConfig.Inference base, AgentConfig.Inference ov) {
    List<AgentConfig.Option> baseOpts = base != null && base.options() != null ? base.options() : List.of();
    List<AgentConfig.Option> ovOpts = ov != null && ov.options() != null ? ov.options() : List.of();
    if (ovOpts.isEmpty()) return baseOpts;
    // Merge by option name, override values present in ov
    Map<String, AgentConfig.Option> map = new LinkedHashMap<>();
    for (AgentConfig.Option o : baseOpts) map.put(o.name(), o);
    for (AgentConfig.Option o : ovOpts) map.put(o.name(), o);
    return new ArrayList<>(map.values());
  }

  private static <T> T firstNonNull(T a, T b) { return a != null ? a : b; }

  // ---------- AutoGen guardrails ----------

  private String generateGuardrails(AgentConfig cfg, Map<String, String> serviceSummaries) {
    StringBuilder prompt = new StringBuilder();
    prompt.append("You are to produce a concise, comprehensive set of guardrails (capabilities and strict boundaries) for an AI Agent.\n");
    prompt.append("Use the provided information only. Anything outside the described capabilities must be refused.\n\n");
    prompt.append("System prompt of the Agent:\n");
    prompt.append(cfg.systemPrompt()).append("\n\n");

    String additional = Optional.ofNullable(cfg.guardrails()).map(AgentConfig.Guardrails::content).orElse("");
    if (!additional.isBlank()) {
      prompt.append("Additional guardrails instructions (to incorporate):\n");
      prompt.append(additional).append("\n\n");
    }

    if (!serviceSummaries.isEmpty()) {
      prompt.append("Available services and operations (OpenAPI-derived):\n");
      for (String s : serviceSummaries.keySet()) {
        prompt.append("- ").append(s).append("\n");
      }
      prompt.append("\n");
    }

    prompt.append("Return only the final guardrails text.\n");

    var resp = inferenceService.sendRequest(prompt.toString(), List.of());
    return Optional.ofNullable(resp).map(r -> r.content()).orElse("");
  }

  private Map<String, String> collectOpenApiServices() {
    return kbService.getServices().orElse(Collections.emptyMap());
  }

  // ---------- Data model ----------

  /** Immutable DTO representing the Agent configuration. */
  public static final class AgentConfig {
    private final String systemPrompt;
    private final Inference inference;
    private final Guardrails guardrails;

    public AgentConfig(String systemPrompt, Inference inference, Guardrails guardrails) {
      this.systemPrompt = systemPrompt; this.inference = inference; this.guardrails = guardrails;
    }
    public String systemPrompt() { return systemPrompt; }
    public Inference inference() { return inference; }
    public Guardrails guardrails() { return guardrails; }

    public static AgentConfig empty() { return new AgentConfig(null, null, null); }

    public AgentConfig withGuardrails(Guardrails g) { return new AgentConfig(systemPrompt, inference, g); }
    public AgentConfig withSystemPrompt(String sp) { return new AgentConfig(sp, inference, guardrails); }

    // YAML mapping helpers
    static AgentConfig fromYaml(YAgent y) {
      return new AgentConfig(
          y.systemPrompt,
          y.inference != null ? new Inference(y.inference.provider, y.inference.model, mapOptions(y.inference.options)) : null,
          y.guardrails != null ? new Guardrails(y.guardrails.autoGen, y.guardrails.content) : null
      );
    }

    private static List<Option> mapOptions(List<YOption> options) {
      if (options == null) return null;
      List<Option> r = new ArrayList<>(options.size());
      for (YOption o : options) r.add(new Option(o.name, o.value));
      return r;
    }

    // Nested records
    public static final class Inference {
      private final String provider; private final String model; private final List<Option> options;
      public Inference(String provider, String model, List<Option> options) {
        this.provider = provider; this.model = model; this.options = options;
      }
      public String provider() { return provider; }
      public String model() { return model; }
      public List<Option> options() { return options; }
    }

    public static final class Option {
      private final String name; private final Object value;
      public Option(String name, Object value) { this.name = name; this.value = value; }
      public String name() { return name; }
      public Object value() { return value; }
    }

    public static final class Guardrails {
      private final Boolean autoGen; private final String content;
      public Guardrails(Boolean autoGen, String content) { this.autoGen = autoGen; this.content = content; }
      public Boolean autoGen() { return autoGen; }
      public String content() { return content; }
      public Guardrails withContent(String c) { return new Guardrails(autoGen, c); }
    }

    // YAML binding tree
    static final class YamlRoot { public YAgent agent; }
    static final class YAgent { public String systemPrompt; public YInference inference; public YGuardrails guardrails; }
    static final class YInference { public String provider; public String model; public List<YOption> options; }
    static final class YOption { public String name; public Object value; }
    static final class YGuardrails { public Boolean autoGen; public String content; }
  }
}
