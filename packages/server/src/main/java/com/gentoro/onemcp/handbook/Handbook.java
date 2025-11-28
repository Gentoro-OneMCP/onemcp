package com.gentoro.onemcp.handbook;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.model.agent.Agent;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.regression.RegressionSuite;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public interface Handbook {
  /**
   * Returns the actual physical location of the handbook. If originally pointing to classpath, it
   * will be mounted on a temporary directory.
   *
   * @return the location of the handbook.
   */
  Path location();

  /**
   * Returns the agent definition, along with all its APIs, guardrails, and regression suites.
   *
   * @return the agent definition.
   */
  Agent agent();

  /**
   * Searches for an API definition at the current Agent, by its slug. Should not find it, returns
   * an empty Optional.
   *
   * @param slug the API slug.
   * @return the API definition, if found.
   */
  Optional<Api> optionalApi(String slug);

  /**
   * Returns the API definition associated with the specified slug. If no API with the given slug
   * exists, this method throws an exception.
   *
   * @param slug the unique identifier (slug) of the API to retrieve.
   * @return the API definition corresponding to the given slug.
   */
  Api api(String slug);

  /**
   * Returns a map of all APIs defined in the current Agent.
   *
   * @return a map of all APIs defined in the current Agent.
   */
  Map<String, Api> apis();

  /**
   * Returns a map of regression suites where the keys are the suite identifiers and the values are
   * the corresponding RegressionSuite objects.
   *
   * @return a map containing all defined regression suites with their identifiers as keys.
   */
  Map<String, RegressionSuite> regressionSuites();

  /**
   * Retrieves an Optional containing a RegressionSuite associated with the specified relative path,
   * if it exists. If no RegressionSuite is mapped to the given path, returns an empty Optional.
   *
   * @param relativePath the relative path used to locate the RegressionSuite.
   * @return an Optional containing the RegressionSuite if found, otherwise an empty Optional.
   */
  Optional<RegressionSuite> optionalRegressionSuite(String relativePath);

  /**
   * Retrieves the RegressionSuite associated with the specified relative path. If no
   * RegressionSuite is found for the given path, this method throws an exception.
   *
   * @param relativePath the relative path used to locate the RegressionSuite.
   * @return the RegressionSuite corresponding to the provided relative path.
   */
  RegressionSuite regressionSuite(String relativePath);

  /**
   * Returns a map containing documentation entries where the keys are the identifiers or titles and
   * the values are the corresponding descriptions or details.
   *
   * @return a map of documentation entries with identifiers as keys and their descriptions as
   *     values.
   */
  Map<String, String> documentation();

  /**
   * Retrieves an Optional containing the documentation associated with the specified relative path,
   * if it exists. If no documentation is found for the given path, returns an empty Optional.
   *
   * @param relativePath the relative path used to locate the desired documentation.
   * @return an Optional containing the documentation if found, otherwise an empty Optional.
   */
  Optional<String> optionalDocumentation(String relativePath);

  /**
   * Retrieves the documentation associated with the specified relative path. If no documentation is
   * found for the given path, it throws an exception.
   *
   * @param relativePath the relative path used to locate the desired documentation.
   * @return the documentation corresponding to the provided relative path.
   */
  String documentation(String relativePath);

  /**
   * Retrieves the OneMcp instance associated with this Handbook.
   *
   * @return the OneMcp instance associated with this Handbook.
   */
  OneMcp oneMcp();

  /**
   * Returns the name associated with this Handbook.
   *
   * @return the name of the handbook.
   */
  String name();
}
