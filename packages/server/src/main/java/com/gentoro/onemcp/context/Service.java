package com.gentoro.onemcp.context;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Service {
  private volatile OneMcp oneMcp;
  private long checksum;
  private String slug;
  private String definitionUri;
  private String resourcesUri;
  private String readmeUri;
  private List<Operation> operations;

  public Service() {
    operations = new ArrayList<>();
  }

  public Service(
      long checksum,
      String slug,
      String definitionUri,
      String resourcesUri,
      String readmeUri,
      List<Operation> operations) {
    this.checksum = checksum;
    this.slug = slug;
    this.definitionUri = definitionUri;
    this.resourcesUri = resourcesUri;
    this.readmeUri = readmeUri;
    this.operations = operations;
  }

  public void setOneMcp(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  public String getReadme(Long ident) {
    return StringUtility.formatWithIndent(
        oneMcp.knowledgeBase().getDocument("kb:///services/" + slug + "/README.md").content(),
        ident.intValue());
  }

  public long getChecksum() {
    return checksum;
  }

  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  public String getSlug() {
    return slug;
  }

  public void setSlug(String slug) {
    this.slug = slug;
  }

  public String getDefinitionUri() {
    return definitionUri;
  }

  public void setDefinitionUri(String definitionUri) {
    this.definitionUri = definitionUri;
  }

  public String getResourcesUri() {
    return resourcesUri;
  }

  public void setResourcesUri(String resourcesUri) {
    this.resourcesUri = resourcesUri;
  }

  public String getReadmeUri() {
    return readmeUri;
  }

  public void setReadmeUri(String readmeUri) {
    this.readmeUri = readmeUri;
  }

  public List<Operation> getOperations() {
    return List.copyOf(operations).stream()
        .peek(
            op -> {
              op.setService(this);
              op.setOneMcp(oneMcp);
            })
        .toList();
  }

  public void setOperations(List<Operation> operations) {
    this.operations = operations;
  }

  public OpenAPI definition(Path handbookDir) {
    return OpenApiLoader.load(handbookDir.resolve(definitionUri).toFile().getAbsolutePath());
  }
}
