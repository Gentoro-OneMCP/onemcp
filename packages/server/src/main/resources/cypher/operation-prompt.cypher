MATCH (op:GraphNode {_key: $operationKey})
OPTIONAL MATCH (op)<-[:HAS_OPERATION]-(e:Entity)
RETURN {
  operation: properties(op),
  entity: properties(e),
  examples: [ (op)-[:HAS_EXAMPLE]->(ex:Example) | properties(ex) ],
  fields: [ (op)-[:HAS_FIELD]->(f:Field) | properties(f) ]
} as result
