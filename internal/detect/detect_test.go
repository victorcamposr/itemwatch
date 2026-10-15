package detect

import (
	"testing"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func birth(origin string, player int64, at time.Time) ledger.Event {
	return ledger.Event{Kind: "birth", Hash: "00000000000000aa", Origin: origin, PlayerID: player, Count: 1, ItemType: 3031, CreatedAt: at}
}

func TestPatternMatch(t *testing.T) {
	cases := []struct {
		pattern, origin string
		want            bool
	}{
		{"*", "anything", true},
		{"loot:", "loot:Dragon", true},
		{"loot:", "loot", false},
		{"market:*", "market:cancel", true},
		{"market", "market:cancel", false},
		{"market", "market", true},
	}
	for _, c := range cases {
		if got := Pattern(c.pattern).Match(c.origin); got != c.want {
			t.Errorf("Pattern(%q).Match(%q) = %v, want %v", c.pattern, c.origin, got, c.want)
		}
	}
}

func TestParseLimitsOrdersBySpecificity(t *testing.T) {
	limits, err := ParseLimits("*=300/10m, loot:=1500/10m, quest:Annihilator=1/24h")
	if err != nil {
		t.Fatal(err)
	}
	want := []Pattern{"quest:Annihilator", "loot:", "*"}
	for i, p := range want {
		if limits[i].Pattern != p {
			t.Fatalf("limit %d = %q, want %q", i, limits[i].Pattern, p)
		}
	}
	if limits[0].Window != 24*time.Hour || limits[0].Max != 1 {
		t.Fatalf("unexpected first limit %+v", limits[0])
	}
}

func TestParseLimitsRejectsGarbage(t *testing.T) {
	for _, spec := range []string{"loot:", "loot:=x/10m", "loot:=5/abc", "loot:=5/10s", "loot:=0/1h"} {
		if _, err := ParseLimits(spec); err == nil {
			t.Errorf("ParseLimits(%q) accepted invalid input", spec)
		}
	}
}

func TestBurstFlagsRepeatedQuestReward(t *testing.T) {
	limits, _ := ParseLimits("quest:=2/24h,*=1000/10m")
	b := NewBurst(limits, ParsePatterns(DefaultIgnoreForTest))

	var findings []Finding
	for i := range 3 {
		findings = append(findings, b.Observe(birth("quest:Annihilator", 7, t0.Add(time.Duration(i)*time.Hour)))...)
	}
	if len(findings) != 1 {
		t.Fatalf("got %d findings, want 1", len(findings))
	}
	f := findings[0]
	if f.Rule != "burst" || f.PlayerID != 7 || f.Facts["events"] != 3 || f.Facts["window"] != "24h" {
		t.Fatalf("unexpected finding %+v", f)
	}

	// The same window does not alert again for the fourth reward.
	if extra := b.Observe(birth("quest:Annihilator", 7, t0.Add(3*time.Hour))); len(extra) != 0 {
		t.Fatalf("alerted twice in the same window: %+v", extra)
	}
	// Another player has its own window.
	if other := b.Observe(birth("quest:Annihilator", 8, t0)); len(other) != 0 {
		t.Fatalf("player 8 alerted on first reward")
	}
}

func TestBurstWindowSlides(t *testing.T) {
	limits, _ := ParseLimits("*=2/10m")
	b := NewBurst(limits, nil)
	// Two items, then a third eleven minutes later: never more than 2 inside 10m.
	for _, at := range []time.Time{t0, t0.Add(time.Minute), t0.Add(12 * time.Minute)} {
		if f := b.Observe(birth("script", 1, at)); len(f) != 0 {
			t.Fatalf("unexpected finding at %v", at)
		}
	}
}

func TestBurstIgnoresMovesAndAnonymousEvents(t *testing.T) {
	limits, _ := ParseLimits("*=1/10m")
	b := NewBurst(limits, ParsePatterns("stash,market:*"))
	events := []ledger.Event{
		birth("stash", 1, t0), birth("stash", 1, t0),
		birth("market:cancel", 1, t0), birth("market:cancel", 1, t0),
		birth("script", 0, t0), birth("script", 0, t0),
		{Kind: "split", Origin: "loot:Rat", PlayerID: 1, CreatedAt: t0},
		{Kind: "split", Origin: "loot:Rat", PlayerID: 1, CreatedAt: t0},
	}
	for _, ev := range events {
		if f := b.Observe(ev); len(f) != 0 {
			t.Fatalf("unexpected finding for %+v", ev)
		}
	}
}

func TestBurstStateRoundTrip(t *testing.T) {
	limits, _ := ParseLimits("*=2/1h")
	b := NewBurst(limits, nil)
	b.Observe(birth("script", 1, t0))
	b.Observe(birth("script", 1, t0.Add(time.Minute)))
	raw, err := b.SaveState()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewBurst(limits, nil)
	if err := restored.LoadState(raw); err != nil {
		t.Fatal(err)
	}
	if f := restored.Observe(birth("script", 1, t0.Add(2*time.Minute))); len(f) != 1 {
		t.Fatalf("restored rule lost its window: %d findings", len(f))
	}
}

func TestScriptOriginsAreSplitBySource(t *testing.T) {
	limits, _ := ParseLimits("*=1/10m")
	b := NewBurst(limits, nil)
	a := birth("script", 1, t0)
	a.Source = "data/scripts/a.lua"
	c := birth("script", 1, t0)
	c.Source = "data/scripts/b.lua"
	if f := append(b.Observe(a), b.Observe(c)...); len(f) != 0 {
		t.Fatalf("two different scripts shared a window")
	}
}

func TestSpikeLearnsBaselineThenFlags(t *testing.T) {
	s := NewSpike(SpikeConfig{Bucket: time.Minute, Alpha: 0.1, K: 4, MinCount: 20, MinFactor: 3, Warmup: 10})
	at := t0
	// 30 quiet minutes with 5 items each.
	for m := range 30 {
		for range 5 {
			if f := s.Observe(birth("loot:Dragon", 1, at.Add(time.Duration(m)*time.Minute))); len(f) != 0 {
				t.Fatalf("alert during normal traffic at minute %d", m)
			}
		}
	}
	// Minute 30: 100 items.
	var findings []Finding
	for range 100 {
		findings = append(findings, s.Observe(birth("loot:Dragon", 1, at.Add(30*time.Minute)))...)
	}
	if len(findings) != 1 {
		t.Fatalf("got %d findings, want exactly 1 per bucket", len(findings))
	}
	if findings[0].Facts["count"].(int) < 20 {
		t.Fatalf("alerted before MinCount: %+v", findings[0].Facts)
	}
}

func TestSpikeNeedsWarmup(t *testing.T) {
	s := NewSpike(SpikeConfig{Warmup: 30, MinCount: 5})
	for range 500 {
		if f := s.Observe(birth("script", 1, t0)); len(f) != 0 {
			t.Fatal("alerted on a brand new origin")
		}
	}
}

func TestSpikeIgnoresServerDowntime(t *testing.T) {
	s := NewSpike(SpikeConfig{Bucket: time.Minute, Alpha: 0.1, K: 4, MinCount: 20, MinFactor: 3, Warmup: 10})
	minute := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	for m := range 30 {
		for range 40 {
			s.Observe(birth("loot:Dragon", 1, minute(m)))
		}
	}
	// The server is down for an hour, then the same traffic resumes.
	var findings []Finding
	for m := 90; m < 95; m++ {
		for range 40 {
			findings = append(findings, s.Observe(birth("loot:Dragon", 1, minute(m)))...)
		}
	}
	if len(findings) != 0 {
		t.Fatalf("normal traffic after a restart flagged as a spike: %+v", findings[0].Facts)
	}
	if r := s.state.Origins["loot:Dragon"]; r.Mean < 30 {
		t.Fatalf("downtime folded into the baseline: mean %.2f", r.Mean)
	}
}

func TestSpikeStillFoldsQuietOriginWhileServerIsUp(t *testing.T) {
	s := NewSpike(SpikeConfig{Bucket: time.Minute, Alpha: 0.5, Warmup: 1})
	s.Observe(birth("boss:Ferumbras", 1, t0))
	// Other origins keep the server visibly up for ten minutes.
	for m := 1; m <= 10; m++ {
		s.Observe(birth("loot:Rat", 1, t0.Add(time.Duration(m)*time.Minute)))
	}
	s.Observe(birth("boss:Ferumbras", 1, t0.Add(10*time.Minute)))
	if r := s.state.Origins["boss:Ferumbras"]; r.N != 10 {
		t.Fatalf("quiet minutes of a live server not folded: n=%d", r.N)
	}
}

func TestDupeTrackerWaitsForConfirmation(t *testing.T) {
	d := NewDupeTracker(20 * time.Minute)
	dup := []ledger.Duplicate{{Hash: "abc", Holders: []ledger.Holder{{Place: "inventory", PlayerID: 1}, {Place: "depot", PlayerID: 2}}}}

	if f := d.Scan(t0, dup); len(f) != 0 {
		t.Fatal("reported on first sighting")
	}
	if f := d.Scan(t0.Add(10*time.Minute), dup); len(f) != 0 {
		t.Fatal("reported before confirmation")
	}
	f := d.Scan(t0.Add(21*time.Minute), dup)
	if len(f) != 1 || f[0].Severity != Critical || f[0].ItemHash != "abc" {
		t.Fatalf("want one critical finding, got %+v", f)
	}
	if again := d.Scan(t0.Add(30*time.Minute), dup); len(again) != 0 {
		t.Fatal("reported twice")
	}
}

func TestDupeTrackerForgetsTransientCopies(t *testing.T) {
	d := NewDupeTracker(20 * time.Minute)
	dup := []ledger.Duplicate{{Hash: "abc"}}
	d.Scan(t0, dup)
	d.Scan(t0.Add(5*time.Minute), nil) // the old owner saved: gone
	if f := d.Scan(t0.Add(25*time.Minute), dup); len(f) != 0 {
		t.Fatal("a copy that disappeared kept its first sighting")
	}
	if d.Tracked() != 1 {
		t.Fatalf("tracked = %d", d.Tracked())
	}
}

func TestUnhashedGroupsPerDay(t *testing.T) {
	ev := ledger.Event{Kind: "unhashed", ItemType: 100, Origin: "script", Source: "x.lua", CreatedAt: t0}
	a := Unhashed{}.Observe(ev)[0]
	ev.CreatedAt = t0.Add(time.Hour)
	b := Unhashed{}.Observe(ev)[0]
	ev.CreatedAt = t0.Add(24 * time.Hour)
	c := Unhashed{}.Observe(ev)[0]
	if a.Key != b.Key || a.Key == c.Key {
		t.Fatalf("keys: %s %s %s", a.Key, b.Key, c.Key)
	}
}

func TestStaffCreation(t *testing.T) {
	if f := (StaffCreation{}).Observe(birth("loot:Rat", 1, t0)); len(f) != 0 {
		t.Fatal("non staff origin flagged")
	}
	f := (StaffCreation{}).Observe(birth("gm:Admin /i 3031 100", 1, t0))
	if len(f) != 1 || f[0].Severity != Info {
		t.Fatalf("got %+v", f)
	}
}

const DefaultIgnoreForTest = "world,change,bank,stash,market,market:*"
