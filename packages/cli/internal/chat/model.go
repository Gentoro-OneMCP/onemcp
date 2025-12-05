package chat

type MCPResponse struct {
	Parts      []MCPAssignmentPart `json:"parts"`
	Statistics MCPStats            `json:"statistics"`
	Context    *MCPContext         `json:"context"`
}

type MCPAssignmentPart struct {
	IsSupported bool   `json:"isSupported"`
	Assignment  string `json:"assignment"`
	IsError     bool   `json:"isError"`
	Content     string `json:"content,omitempty"`
}

// STATISTICS INCLUDING TRACE
type MCPStats struct {
	PromptTokens     int           `json:"promptTokens"`
	CompletionTokens int           `json:"completionTokens"`
	TotalTokens      int           `json:"totalTokens"`
	TotalTimeMs      int           `json:"totalTimeMs"`
	Operations       []string      `json:"operations"`
	Trace            *MCPTraceNode `json:"trace,omitempty"`
}

type MCPTraceNode struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	StartMs    int64                  `json:"startMs"`
	EndMs      int64                  `json:"endMs"`
	DurationMs int64                  `json:"durationMs"`
	Status     string                 `json:"status"`
	Attributes map[string]interface{} `json:"attributes"`
	Children   []MCPTraceNode         `json:"children"`
}

// FINAL CONTEXT BLOCK
type MCPContext struct {
	RefinedAssignment string             `json:"refinedAssignment"`
	UnhandledParts    string             `json:"unhandledParts"`
	Context           []MCPContextEntity `json:"context"`
}

type MCPContextEntity struct {
	Entity     string   `json:"entity"`
	Operations []string `json:"operations"`
	Confidence int      `json:"confidence"`
	Referral   string   `json:"referral"`
}
