package config

import (
	"errors"
	"strings"
)

func validateSharedSchemaGroups(groups []string) error {
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if group == "*" {
			if len(groups) != 1 {
				return errors.New("phase_two access shared schema wildcard must be the only query group")
			}
			continue
		}
		if !canonicalText(group) || len(group) > 256 {
			return errors.New("phase_two access shared schema query group is invalid")
		}
		for _, c := range group {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("_.:-", c)) {
				return errors.New("phase_two access shared schema query group is invalid")
			}
		}
		if _, duplicate := seen[group]; duplicate {
			return errors.New("phase_two access shared schema query group is duplicated")
		}
		seen[group] = struct{}{}
	}
	return nil
}
