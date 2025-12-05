package chat

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// PrettyPrintTrace writes a nicely formatted trace tree into an io.Writer (file, logger, etc.)
func PrettyPrintTrace(w io.Writer, trace *MCPTraceNode) {
	if trace == nil {
		fmt.Fprintln(w, "No trace information available.")
		return
	}

	writeTraceNode(w, trace, 0)
}

func writeTraceNode(w io.Writer, node *MCPTraceNode, indent int) {
	prefix := strings.Repeat("  ", indent)

	// Basic node header
	fmt.Fprintf(
		w,
		"%s[%s] %s (%dms)\n",
		prefix,
		node.Name,
		node.Status,
		node.DurationMs,
	)

	// Attributes
	if len(node.Attributes) > 0 {
		fmt.Fprintf(w, "%s  Attributes:\n", prefix)
		for k, v := range node.Attributes {
			// Pretty JSON for complex values
			pretty := formatValue(v)
			fmt.Fprintf(w, "%s    %s = %s\n", prefix, k, pretty)
		}
	}

	// Children
	if len(node.Children) > 0 {
		fmt.Fprintf(w, "%s  Children:\n", prefix)
		for _, child := range node.Children {
			writeTraceNode(w, &child, indent+2)
		}
	}
}

// Helper formats attribute values with JSON for safety
func formatValue(v interface{}) string {
	switch v := v.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	default:
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}
