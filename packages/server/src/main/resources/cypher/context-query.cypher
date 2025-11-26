MATCH (e:GraphNode)
WHERE e.name = $entityName OR e.title = $entityName
WITH e
OPTIONAL MATCH (e)-[:HAS_OPERATION]->(op:GraphNode)
RETURN {
  entity: properties(e),
  operations: collect(properties(op))
} as result
