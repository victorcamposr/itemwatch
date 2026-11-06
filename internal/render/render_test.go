package render

import (
	"strings"
	"testing"
	"time"

	"github.com/victorcamposr/itemwatch/internal/detect"
)

func TestProbeTitles(t *testing.T) {
	for _, check := range []string{"ot-status", "http-login", "db-write"} {
		f := detect.ProbeFinding(check, time.Now(), "technical detail")
		en, pt := Title(English, f), Title(Portuguese, f)
		if en == "" || pt == "" || en == pt {
			t.Fatalf("%s: en %q, pt %q", check, en, pt)
		}
		if strings.Contains(en, "technical detail") || strings.Contains(pt, "technical detail") {
			t.Fatalf("%s: the title must stay plain: %q", check, en)
		}
		if strings.Contains(en, check) {
			t.Fatalf("%s: fell back to the generic text", check)
		}
	}
}

func TestUnknownProbeNamesTheCheck(t *testing.T) {
	f := detect.ProbeFinding("new-check", time.Now(), "")
	if got := Title(Portuguese, f); !strings.Contains(got, "new-check") {
		t.Fatalf("title %q", got)
	}
}
