package handbook

type TestSpec struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Tests   []Test `yaml:"tests"`
}

type Test struct {
	Prompt      string `yaml:"prompt"`
	DisplayName string `yaml:"display-name"`
	Assert      string `yaml:"assert"`
}
