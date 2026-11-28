package ops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// nameKey is the JSON Schema keyword that marks a string field as a name or not.
const nameKey = "x-rock-solid-name"

// schemaOf is the JSON Schema of t, at path in its document; a string field without a name tag
// is an error.
func schemaOf(t reflect.Type, tag reflect.StructTag, path string) (map[string]any, error) {
	stringy := t.Kind() == reflect.String || strings.Contains(tag.Get("json"), ",string")
	if stringy {
		mark := tag.Get("name")
		if mark != "yes" && mark != "no" {
			return nil, fmt.Errorf("%s: a string field with no name:\"yes\" or name:\"no\" tag", path)
		}
		s := map[string]any{"type": "string", nameKey: mark == "yes"}
		if t.Kind() != reflect.String {
			s["pattern"] = "^[0-9]+$"
		}
		return s, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer", "minimum": 0}, nil
	case reflect.Pointer:
		return schemaOf(t.Elem(), tag, path)
	case reflect.Slice:
		items, err := schemaOf(t.Elem(), tag, path+"[]")
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case reflect.Struct:
		props := map[string]any{}
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			s, err := schemaOf(f.Type, f.Tag, path+"."+name)
			if err != nil {
				return nil, err
			}
			props[name] = s
		}
		return map[string]any{"type": "object", "properties": props}, nil
	}
	return nil, fmt.Errorf("%s: no schema for %v", path, t)
}

func document(id string, v any) ([]byte, error) {
	s, err := schemaOf(reflect.TypeOf(v), "", "")
	if err != nil {
		return nil, err
	}
	s["$schema"], s["$id"] = "https://json-schema.org/draft/2020-12/schema", id
	b, err := json.MarshalIndent(s, "", "  ")
	return append(b, '\n'), err
}

// facts-v1.schema.json and facts-pg-v1.schema.json are the frozen schemas; the test reads them,
// never the reverse. Within v1 a change only adds a field.
func TestFactsSchemaGolden(t *testing.T) {
	for _, c := range []struct {
		file, id string
		v        any
	}{{"facts-v1.schema.json", FactsV1, Facts{}}, {"facts-pg-v1.schema.json", PGFactsV1, PGFacts{}}} {
		got, err := document(c.id, c.v)
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs from the types; they generate:\n%s", c.file, got)
		}
	}
}

func TestEveryStringFieldMarked(t *testing.T) {
	for _, v := range []any{Facts{}, PGFacts{}} {
		if _, err := document("", v); err != nil {
			t.Error(err)
		}
	}
	type added struct {
		Facts
		Pod string `json:"pod"`
	}
	if _, err := document("", added{}); err == nil || !strings.Contains(err.Error(), ".pod") {
		t.Fatalf("a string field added without a name mark: %v", err)
	}
	type number struct {
		ID uint64 `json:"id,string"`
	}
	if _, err := document("", number{}); err == nil {
		t.Fatal("a number encoded as a string passed without a name mark")
	}
}

func TestFactsBuiltOncePerSecond(t *testing.T) {
	var c Cache
	builds := 0
	get := func(now uint64) {
		if c.Fresh(core.Tick(now)) == nil {
			builds++
			c.Keep(core.Tick(now), []byte("{}"))
		}
	}
	for _, now := range []uint64{5_000_000, 5_000_001, 5_400_000, 5_999_999} {
		get(now)
	}
	if builds != 1 {
		t.Fatalf("four requests within a second built %d answers", builds)
	}
	get(6_000_000)
	if builds != 2 {
		t.Fatal("a request a second after the answer was built took it")
	}
}

func sample() Facts {
	return Facts{Schema: FactsV1,
		Node: Node{ID: 2, Release: 1, Roles: []string{"storage"}, Run: 1 << 60, UptimeUs: 9, BootUs: 10, RawUs: 11, Wall: "2026-11-29T00:00:00Z",
			Flags: []Flag{{"listen", "127.0.0.1:7000"}}, Hosts: []Host{{1, "127.0.0.1:6000", true}}},
		Map:      Map{Epoch: 3, Sessions: 4, ShardMap: 5, ReportsTo: 1},
		Disks:    []Disk{{Loop: 1, Path: "/dev/disk/by-id/a", Device: 1<<64 - 1, State: "open", Disk: 4, Log: 7, MemberAckUs: []Latency{{4, 100}}}},
		Members:  []Member{{Disk: 4, Pool: 1, PG: 2, Interval: 3, Role: "primary", Active: true}},
		Replicas: []Replica{{Group: 2, ID: 9, Loop: 1, Term: 3, Leader: 9, Commit: 10, Applied: 10, Lease: true, Followers: []Follower{{8, 10}}}},
		Session:  Session{ID: 5, Incarnation: 1, LeaseLeftUs: 3_000_000},
		Mounts:   []Mount{{1, "/mnt/v"}},
	}
}

func TestAV1AnswerDecodes(t *testing.T) {
	f := sample()
	b, err := json.Marshal(&f)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFacts(append(b[:len(b)-1], []byte(`,"added_later":{"x":1}}`)...))
	if err != nil || !reflect.DeepEqual(got, f) {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	if _, err := DecodeFacts([]byte(`{"schema":"rock-solid.io/facts/v2"}`)); err == nil {
		t.Fatal("a v2 answer decoded as v1")
	}
}

func FuzzDecodeFacts(f *testing.F) {
	s := sample()
	b, _ := json.Marshal(&s)
	f.Add(b)
	f.Add([]byte(`{"schema":"rock-solid.io/facts/v1"}`))
	f.Add([]byte(`{"schema":"rock-solid.io/facts/v1","disks":[{"device":"18446744073709551616"}]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeFacts(b)
		if err != nil {
			return
		}
		again, err := json.Marshal(&got)
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodeFacts(again)
		if err != nil {
			t.Fatalf("%s decoded and encoded fails to decode: %v", again, err)
		}
		if twice, _ := json.Marshal(&back); !bytes.Equal(twice, again) {
			t.Fatalf("%s decoded, encoded and decoded again encodes as %s", again, twice)
		}
	})
}
