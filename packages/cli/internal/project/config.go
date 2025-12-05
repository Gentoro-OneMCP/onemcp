package project

import (
	"os"

	"gopkg.in/yaml.v3"
)

// ----------------------------------------
// Root
// ----------------------------------------

type Spec struct {
	Releases   []Release  `yaml:"releases"`
	Guardrails Guardrails `yaml:"guardrails"`
	APIs       []API      `yaml:"apis"`
}

// ----------------------------------------
// Releases
// ----------------------------------------

type Release struct {
	Version     string `yaml:"version"`
	Author      Author `yaml:"author"`
	Date        string `yaml:"date"`
	Description string `yaml:"description"`
}

type Author struct {
	Name  string `yaml:"name"`
	Email string `yaml:"email"`
}

// ----------------------------------------
// Guardrails
// ----------------------------------------

type Guardrails struct {
	Policies []Policy `yaml:"policies"`
}

type Policy struct {
	ID          string   `yaml:"id"`
	Phase       []string `yaml:"phase"`
	Description string   `yaml:"description"`
}

// ----------------------------------------
// APIs
// ----------------------------------------

type API struct {
	Slug        string   `yaml:"slug"`
	Name        string   `yaml:"name"`
	Ref         string   `yaml:"ref"`
	BaseURLs    []string `yaml:"baseUrls"`
	Description string   `yaml:"description"`
	Entities    []Entity `yaml:"entities"`
}

// ----------------------------------------
// Entities
// ----------------------------------------

type Entity struct {
	Name          string         `yaml:"name"`
	Aliases       []Alias        `yaml:"aliases"`
	OpenAPITag    string         `yaml:"openApiTag"`
	Description   string         `yaml:"description"`
	Relationships []Relationship `yaml:"relationships"`
	Operations    []Operation    `yaml:"operations"`
}

type Alias struct {
	Terms       []string `yaml:"terms"`
	Description string   `yaml:"description"`
}

type Relationship struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Description string `yaml:"description"`
}

type Operation struct {
	Kind        string `yaml:"kind"`
	Description string `yaml:"description"`
}

// ----------------------------------------
// Loader
// ----------------------------------------

// LoadConfig loads the YAML file at the given path into a Spec.
// If the file does NOT exist, it returns an empty Spec with initialized slices.
func LoadConfig(path string) (*Spec, error) {
	// If file doesn't exist → return empty but initialized Spec.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return emptySpec(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var spec Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}

	ensureNonNilSlices(&spec)
	return &spec, nil
}

// ----------------------------------------
// Helpers
// ----------------------------------------

// emptySpec returns a fully initialized empty Spec — no nil slices.
func emptySpec() *Spec {
	return &Spec{
		Releases: []Release{},
		Guardrails: Guardrails{
			Policies: []Policy{},
		},
		APIs: []API{},
	}
}

// ensureNonNilSlices walks the struct and initializes any nil slices.
func ensureNonNilSlices(spec *Spec) {
	if spec.Releases == nil {
		spec.Releases = []Release{}
	}

	if spec.Guardrails.Policies == nil {
		spec.Guardrails.Policies = []Policy{}
	}

	if spec.APIs == nil {
		spec.APIs = []API{}
	}

	for i := range spec.APIs {
		api := &spec.APIs[i]

		if api.BaseURLs == nil {
			api.BaseURLs = []string{}
		}
		if api.Entities == nil {
			api.Entities = []Entity{}
		}

		for j := range api.Entities {
			ent := &api.Entities[j]

			if ent.Aliases == nil {
				ent.Aliases = []Alias{}
			}
			if ent.Relationships == nil {
				ent.Relationships = []Relationship{}
			}
			if ent.Operations == nil {
				ent.Operations = []Operation{}
			}

			for k := range ent.Aliases {
				if ent.Aliases[k].Terms == nil {
					ent.Aliases[k].Terms = []string{}
				}
			}
			for k := range ent.Relationships {
				// no slices inside Relationship
				_ = ent.Relationships[k]
			}
		}

		for j := range spec.Guardrails.Policies {
			pol := &spec.Guardrails.Policies[j]
			if pol.Phase == nil {
				pol.Phase = []string{}
			}
		}
	}
}
