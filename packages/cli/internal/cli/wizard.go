package cli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
)

type Mode int

const (
	ModeLocal Mode = iota
	ModeRemote
)

type ModeOption struct {
	Value Mode
	Label string
}

var modeOptions = []ModeOption{
	{
		Value: ModeLocal,
		Label: "Local: Start a local docker container and deploy OneMCP.",
	},
	{
		Value: ModeRemote,
		Label: "Remote: Connect with an already running OneMCP instance.",
	},
}

func defaultShortLabel(m Mode) string {
	if m == ModeRemote {
		return "remote"
	}
	return "local"
}

func defaultLabel(m Mode) string {
	for _, opt := range modeOptions {
		if opt.Value == m {
			return opt.Label
		}
	}
	return ""
}

func modeFromLabel(label string) Mode {
	for _, opt := range modeOptions {
		if opt.Label == label {
			return opt.Value
		}
	}
	return ModeLocal // safe default
}

func selectMode(defaultMode Mode) (Mode, error) {
	var selection string

	// Build label array for Survey
	labels := make([]string, len(modeOptions))
	for i, opt := range modeOptions {
		labels[i] = opt.Label
	}

	prompt := &survey.Select{
		Message: "How would you like to initialize:",
		Options: labels,
		Default: defaultLabel(defaultMode), // 👈 enum-safe default
	}

	if err := survey.AskOne(prompt, &selection, survey.WithValidator(survey.Required)); err != nil {
		return 0, err
	}

	// Convert back to enum
	return modeFromLabel(selection), nil
}

func selectTcpPort(defaultPort int) (int, error) {
	// Convert default to string for Survey
	portStr := strconv.Itoa(defaultPort)

	prompt := &survey.Input{
		Message: "Enter TCP port:",
		Default: portStr,
	}

	validator := func(val interface{}) error {
		str, ok := val.(string)
		if !ok {
			return errors.New("invalid input")
		}

		str = strings.TrimSpace(str)

		// must be a number
		num, err := strconv.Atoi(str)
		if err != nil {
			return errors.New("must be a number")
		}

		// must be a valid TCP port
		if num < 1 || num > 65535 {
			return errors.New("port must be between 1 and 65535")
		}

		return nil
	}

	// Ask and update portStr
	if err := survey.AskOne(prompt, &portStr, survey.WithValidator(validator)); err != nil {
		return 0, err
	}

	// Convert final answer to int
	tcpPort, _ := strconv.Atoi(portStr)
	return tcpPort, nil
}

func selectOption(message string, options []string, defaultOption string) (string, error) {
	// Validate the default value exists in options
	defaultExists := false
	for _, opt := range options {
		if opt == defaultOption {
			defaultExists = true
			break
		}
	}
	if !defaultExists && defaultOption != "" {
		return "", fmt.Errorf("default option %q not in option list", defaultOption)
	}

	var selected string

	prompt := &survey.Select{
		Message: message,
		Options: options,
		Default: defaultOption, // Defaults to empty if none is provided
	}

	if err := survey.AskOne(prompt, &selected, survey.WithValidator(survey.Required)); err != nil {
		return "", err
	}

	return selected, nil
}

func provideSensitiveInput(message string) (string, error) {
	content := ""
	prompt := &survey.Password{
		Message: message,
	}
	if err := survey.AskOne(prompt, &content, survey.WithValidator(survey.Required)); err != nil {
		return "", err
	}

	return content, nil
}

func provideInput(message string, defaultValue string) (string, error) {
	content := ""
	prompt := &survey.Input{
		Message: message,
		Default: defaultValue,
	}
	if err := survey.AskOne(prompt, &content, survey.WithValidator(survey.Required)); err != nil {
		return "", err
	}

	content = strings.TrimSpace(content)
	if content == "" {
		content = defaultValue
	}

	return content, nil
}
