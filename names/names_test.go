package names

import "testing"

func TestExpand(t *testing.T) {
	cases := []struct{ name, node, ns, want string }{
		{"chatter", "talker", "/", "/chatter"},
		{"chatter", "talker", "/robot1", "/robot1/chatter"},
		{"/abs/topic", "talker", "/robot1", "/abs/topic"},
		{"~/status", "talker", "/robot1", "/robot1/talker/status"},
		{"~", "talker", "/", "/talker"},
		{"{node}/foo", "talker", "/", "/talker/foo"},
		{"{ns}/foo", "talker", "/a/b", "/a/b/foo"},
		{"{ns}/foo", "talker", "/", "/foo"},
	}
	for _, c := range cases {
		got, err := Expand(c.name, c.node, c.ns)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestInvalid(t *testing.T) {
	bad := []string{"", "a//b", "a/", "1abc", "a/2b", "a~b", "sp ace", "{x", "x}"}
	for _, b := range bad {
		if err := ValidateTopic(b); err == nil {
			t.Fatalf("%q should be invalid", b)
		}
	}
	if err := ValidateNodeName("my/node"); err == nil {
		t.Fatal("node name with slash")
	}
	if err := ValidateNamespace("rel"); err == nil {
		t.Fatal("relative namespace")
	}
	if err := ValidateNamespace("/a/b"); err != nil {
		t.Fatal(err)
	}
}

func TestHidden(t *testing.T) {
	if !IsHidden("/foo/_bar") || IsHidden("/rosout") {
		t.Fatal("hidden")
	}
}
