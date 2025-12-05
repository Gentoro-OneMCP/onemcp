package chat

import (
	"encoding/json"
	"strconv"

	"github.com/onemcp/cli/internal/project"
)

// ----------------------------------------
// JSON Output Structures
// ----------------------------------------

type OutputContextEnvelope struct {
	Contexts []OutputContext `json:"context"`
}

type OutputContext struct {
	Kind    string         `json:"kind"`
	Headers []OutputHeader `json:"headers"`
	Cookies []OutputCookie `json:"cookies"`
}

type OutputHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type OutputCookie struct {
	Name  string      `json:"name"`
	Value string      `json:"value"`
	Age   interface{} `json:"age"` // numeric or string fallback
	Path  string      `json:"path"`
}

// ----------------------------------------
// Converter
// ----------------------------------------

// BuildContextJSON converts a list of APIContext into the JSON structure:
//
//	{
//	  "context": [
//	    {
//	      "kind": "API_CONTEXT",
//	      "headers": [...],
//	      "cookies": [...]
//	    }
//	  ]
//	}
func BuildContextJSON(contexts []project.APIContext) (string, error) {
	out := OutputContextEnvelope{
		Contexts: []OutputContext{},
	}

	for _, ctx := range contexts {
		converted := OutputContext{
			Kind:    "API_CONTEXT",
			Headers: []OutputHeader{},
			Cookies: []OutputCookie{},
		}

		// Convert Headers
		for _, h := range ctx.Headers {
			converted.Headers = append(converted.Headers, OutputHeader{
				Name:  h.Name,
				Value: h.Value,
			})
		}

		// Convert Cookies (age must be JSON number if numeric)
		for _, c := range ctx.Cookies {
			var age interface{} = c.Age
			if n, err := strconv.Atoi(c.Age); err == nil {
				age = n // convert numeric age → JSON number
			}

			converted.Cookies = append(converted.Cookies, OutputCookie{
				Name:  c.Name,
				Value: c.Value,
				Age:   age,
				Path:  c.Path,
			})
		}

		out.Contexts = append(out.Contexts, converted)
	}

	jsonBytes, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}
