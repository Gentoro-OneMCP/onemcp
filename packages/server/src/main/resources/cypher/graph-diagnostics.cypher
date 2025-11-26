MATCH (n {_key: $operationKey})
RETURN {
  node: properties(n),
  labels: labels(n),
  relationships: [ (n)-[r]-(m) | {type: type(r), target: properties(m)} ]
} as result
