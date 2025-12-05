package project

import (
	"gopkg.in/yaml.v3"
	"os"
)

// ----------------------------------------
// Context Model
// ----------------------------------------

type ContextSpec struct {
	Contexts []APIContext `yaml:"context"`
}

type APIContext struct {
	APISlug string   `yaml:"api_slug"`
	Headers []Header `yaml:"headers"`
	Cookies []Cookie `yaml:"cookies"`
}

type Header struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type Cookie struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
	Age   string `yaml:"age"`
	Path  string `yaml:"path"`
}

// ----------------------------------------
// Loader
// ----------------------------------------

func LoadContext(path string) (*ContextSpec, error) {
	// If file does not exist → return empty initialized structure
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return emptyContextSpec(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var spec ContextSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}

	ensureContextNonNilSlices(&spec)
	return &spec, nil
}

// ----------------------------------------
// Helpers
// ----------------------------------------

func emptyContextSpec() *ContextSpec {
	return &ContextSpec{
		Contexts: []APIContext{},
	}
}

func ensureContextNonNilSlices(spec *ContextSpec) {
	if spec.Contexts == nil {
		spec.Contexts = []APIContext{}
	}

	for i := range spec.Contexts {
		ctx := &spec.Contexts[i]

		if ctx.Headers == nil {
			ctx.Headers = []Header{}
		}
		if ctx.Cookies == nil {
			ctx.Cookies = []Cookie{}
		}
	}
}

// GetContextFor returns the APIContext that matches the given API slug.
// Returns (nil, false) if no context exists.
func (c *ContextSpec) GetContextFor(slug string) (*APIContext, bool) {
	for i := range c.Contexts {
		ctx := &c.Contexts[i]
		if ctx.APISlug == slug {
			// Ensure slices are non-nil even if YAML omitted them
			if ctx.Headers == nil {
				ctx.Headers = []Header{}
			}
			if ctx.Cookies == nil {
				ctx.Cookies = []Cookie{}
			}
			return ctx, true
		}
	}
	return nil, false
}

// GetContextsForAPIs returns all API contexts whose api_slug matches
// any API.Slug from the provided list.
func (c *ContextSpec) GetContextsForAPIs(apis []API) []APIContext {
	if c == nil || len(c.Contexts) == 0 || len(apis) == 0 {
		return []APIContext{}
	}

	// Build a quick lookup map of valid slugs
	slugSet := make(map[string]struct{}, len(apis))
	for _, api := range apis {
		slugSet[api.Slug] = struct{}{}
	}

	matches := []APIContext{}

	for _, ctx := range c.Contexts {
		if _, ok := slugSet[ctx.APISlug]; ok {
			// Ensure slices are non-nil
			if ctx.Headers == nil {
				ctx.Headers = []Header{}
			}
			if ctx.Cookies == nil {
				ctx.Cookies = []Cookie{}
			}
			matches = append(matches, ctx)
		}
	}

	return matches
}
