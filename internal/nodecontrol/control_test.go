package nodecontrol

import (
	"testing"
)

func TestClosedCommandVocabulary(t *testing.T) {
	for _, s := range []string{"help", "status", "resources", "logs", "logs 1000", "start", "stop", "restart"} {
		if _, e := Parse(s); e != nil {
			t.Fatal(s, e)
		}
	}
	for _, s := range []string{"sh", "sudo reboot", "status; rm -rf /", "logs 1001", "logs -1", "logs 10 --follow", "$(id)", "status extra", "", "install /tmp/x"} {
		if _, e := Parse(s); e == nil {
			t.Error("accepted", s)
		}
	}
}
