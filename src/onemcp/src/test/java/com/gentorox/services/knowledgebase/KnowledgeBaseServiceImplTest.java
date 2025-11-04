package com.gentorox.services.knowledgebase;

import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.typescript.TypescriptRuntimeClient;
import com.gentorox.services.typescript.TypescriptRuntimeClient.DocsResponse;
import com.gentorox.services.typescript.TypescriptRuntimeClient.DocFile;
import com.gentorox.services.typescript.TypescriptRuntimeClient.Sdk;
import com.gentorox.services.typescript.TypescriptRuntimeClient.UploadResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class KnowledgeBaseServiceImplTest {

  InferenceService inference;
  TypescriptRuntimeClient ts;
  KnowledgeBasePersistence persistence;

  @BeforeEach
  void setUp() {
    inference = Mockito.mock(InferenceService.class);
    // Return a short deterministic hint regardless of prompt
    when(inference.sendRequest(any(), any())).thenReturn(new InferenceResponse("test hint", Optional.empty(), "trace"));

    ts = Mockito.mock(TypescriptRuntimeClient.class);

    persistence = new FileKnowledgeBasePersistence();
  }

  private static void setStateFile(KnowledgeBaseServiceImpl svc, Path stateFile) throws Exception {
    Field f = KnowledgeBaseServiceImpl.class.getDeclaredField("stateFile");
    f.setAccessible(true);
    f.set(svc, stateFile);
  }

  @Test
  void ingestsFoundationAndListsAndGetsContent(@TempDir Path tmp) throws Exception {
    // Create foundation structure
    Path foundation = tmp.resolve("foundation");
    Files.createDirectories(foundation);

    Path docs = Files.createDirectories(foundation.resolve("docs"));
    Path doc1 = docs.resolve("documentation_1.md");
    Files.writeString(doc1, "Doc1 content");

    Path feedback = Files.createDirectories(foundation.resolve("feedback"));
    Path fb = feedback.resolve("notes.txt");
    Files.writeString(fb, "Some feedback");

    Path tests = Files.createDirectories(foundation.resolve("tests"));
    Path t1 = tests.resolve("test1.txt");
    Files.writeString(t1, "Test data");

    KnowledgeBaseServiceImpl svc = new KnowledgeBaseServiceImpl(inference, ts, persistence);
    setStateFile(svc, tmp.resolve("kb/state.json"));

    svc.initialize(foundation);

    // Verify entries
    var entries = svc.list(null);
    assertTrue(entries.size() >= 3, "Expected at least 3 entries");

    // Check listing by prefix
    String docsPrefix = "kb://docs";
    var docsEntries = svc.list(docsPrefix);
    assertFalse(docsEntries.isEmpty());
    assertTrue(docsEntries.stream().allMatch(e -> e.resource().startsWith(docsPrefix)));

    // getContent for a KB resource
    String fileRes = "kb://docs/" + doc1.getFileName().toString();
    Optional<String> content = svc.getContent(fileRes);
    assertTrue(content.isPresent());
    assertEquals("Doc1 content", content.get());

    // No TS client interactions in this test
    verifyNoInteractions(ts);
  }

  @Test
  void usesCacheOnSecondInitialization(@TempDir Path tmp) throws Exception {
    Path foundation = tmp.resolve("foundation");
    Files.createDirectories(foundation);
    Files.writeString(foundation.resolve("Agent.md"), "# Agent\nBehave well.");

    KnowledgeBaseServiceImpl svc = new KnowledgeBaseServiceImpl(inference, ts, persistence);
    Path statePath = tmp.resolve("kb/state.json");
    setStateFile(svc, statePath);

    // First run builds and saves
    svc.initialize(foundation);
    assertFalse(svc.loadedFromCache());
    assertTrue(Files.exists(statePath));

    // Clear invocations and re-init, should load from cache and avoid inference calls
    clearInvocations(inference);

    KnowledgeBaseServiceImpl svc2 = new KnowledgeBaseServiceImpl(inference, ts, persistence);
    setStateFile(svc2, statePath);
    svc2.initialize(foundation);

    assertTrue(svc2.loadedFromCache());
    verifyNoInteractions(ts);
    verifyNoInteractions(inference);
  }

  @Test
  void processesOpenApiSpecsAndCreatesMemDocs(@TempDir Path tmp) throws Exception {
    Path foundation = tmp.resolve("foundation");
    Files.createDirectories(foundation);
    Path specs = Files.createDirectories(foundation.resolve("openapi"));
    Path spec = specs.resolve("api.json");
    Files.writeString(spec, "{\n  \"openapi\": \"3.0.0\",\n  \"info\": {\"title\": \"Sample API\"},\n  \"paths\": {}\n}");

    // Mock upload + docs
    when(ts.uploadOpenapi(eq(spec), any(), anyBoolean())).thenReturn(Mono.just(new UploadResult(true, new Sdk("ns1", "/tmp/sdk/ns1", "file:///tmp/sdk/ns1/index.ts"), "ok")));
    List<DocFile> files = List.of(new DocFile("ServiceApi.md", "# Service API\nDetails."));
    when(ts.fetchDocs(eq("ns1"), eq(false))).thenReturn(Mono.just(new DocsResponse(true, "ns1", files.size(), files, null, "generated")));

    KnowledgeBaseServiceImpl svc = new KnowledgeBaseServiceImpl(inference, ts, persistence);
    setStateFile(svc, tmp.resolve("kb/state.json"));

    svc.initialize(foundation);

    var entries = svc.list("kb://openapi/ns1");
    assertEquals(1, entries.size());
    KnowledgeBaseEntry e = entries.get(0);
    assertTrue(e.resource().equals("kb://openapi/ns1/docs/ServiceApi.md"));

    // Ensure getContent works for kb resource
    var content = svc.getContent(e.resource());
    assertTrue(content.isPresent());
    assertTrue(content.get().contains("Service API"));

    // Verify TS client interacted
    verify(ts, times(1)).uploadOpenapi(eq(spec), any(), anyBoolean());
    verify(ts, times(1)).fetchDocs(eq("ns1"), eq(false));
  }
}
