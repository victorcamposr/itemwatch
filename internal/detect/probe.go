package detect

import "time"

// ProbeRule is the rule name of the alerts raised by the health checks.
const ProbeRule = "probe"

// ProbeKey is the dedup key of a health check: one incident per check, so a
// check that fails again reopens the same alert with a new generation.
func ProbeKey(check string) string { return dedupKey(ProbeRule, check) }

// ProbeFinding describes a health check that has been failing since a given
// moment. The check name goes in Origin, which picks the rendered text.
func ProbeFinding(check string, since time.Time, reason string) Finding {
	return Finding{
		Rule:     ProbeRule,
		Severity: Critical,
		Key:      ProbeKey(check),
		At:       since,
		Count:    1,
		Origin:   check,
		Facts:    map[string]any{"check": check, "error": reason, "failing_since": since},
	}
}
