// Package render turns findings into human text in the configured language.
// Alerts are rendered once, when first stored, so the API and every
// notification channel show the same words.
package render

import (
	"fmt"
	"strings"

	"github.com/victorcamposr/itemwatch/internal/detect"
)

// Lang is a supported language tag.
type Lang string

const (
	English    Lang = "en"
	Portuguese Lang = "pt-BR"
)

// ParseLang accepts en and pt-BR (case insensitive); anything else is English.
func ParseLang(s string) Lang {
	if strings.EqualFold(s, "pt-br") || strings.EqualFold(s, "pt") {
		return Portuguese
	}
	return English
}

type catalog struct {
	unhashed, staff, burst, spike, dupe string
	labels                              map[string]string
}

var catalogs = map[Lang]catalog{
	English: {
		unhashed: "Item %v reached the ledger without a hash (origin %s)",
		staff:    "Staff created items with %s",
		burst:    "%v items from %s to one player in %s (limit %v)",
		spike:    "Spike in %s: %v items in %s, usually %v",
		dupe:     "Item %s is in %v places at once",
		labels: map[string]string{
			"player": "Player", "origin": "Origin", "hash": "Hash", "occurrences": "Occurrences",
			"severity": "Severity", "reopened": "Reopened after being resolved",
			"info": "info", "warning": "warning", "critical": "critical",
		},
	},
	Portuguese: {
		unhashed: "Item %v chegou ao registro sem hash (origem %s)",
		staff:    "Staff criou itens com %s",
		burst:    "%v itens de %s para um jogador em %s (limite %v)",
		spike:    "Pico em %s: %v itens em %s, o normal é %v",
		dupe:     "Item %s está em %v lugares ao mesmo tempo",
		labels: map[string]string{
			"player": "Jogador", "origin": "Origem", "hash": "Hash", "occurrences": "Ocorrências",
			"severity": "Gravidade", "reopened": "Reaberto depois de resolvido",
			"info": "info", "warning": "atenção", "critical": "crítico",
		},
	},
}

// Title renders the one-line description of a finding.
func Title(lang Lang, f detect.Finding) string {
	c := catalogFor(lang)
	switch f.Rule {
	case "unhashed":
		return fmt.Sprintf(c.unhashed, f.Facts["item_type"], f.Origin)
	case "staff":
		return fmt.Sprintf(c.staff, f.Origin)
	case "burst":
		return fmt.Sprintf(c.burst, f.Facts["events"], f.Origin, f.Facts["window"], f.Facts["limit"])
	case "spike":
		return fmt.Sprintf(c.spike, f.Origin, f.Facts["count"], f.Facts["bucket"], f.Facts["mean"])
	case "dupe":
		return fmt.Sprintf(c.dupe, f.ItemHash, f.Facts["places"])
	}
	return f.Rule
}

// Label returns a field label ("player", "origin", ...) or a severity name.
func Label(lang Lang, key string) string {
	if s, ok := catalogFor(lang).labels[key]; ok {
		return s
	}
	return key
}

func catalogFor(lang Lang) catalog {
	if c, ok := catalogs[lang]; ok {
		return c
	}
	return catalogs[English]
}
