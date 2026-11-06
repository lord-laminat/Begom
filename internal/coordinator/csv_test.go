package coordinator

import "testing"

func TestCSVFormulaAndPointerSafety(t *testing.T) {
	for _, s := range []string{"=2+2", "\r\n=cmd", " +formula", "\t@SUM(A1:A2)"} {
		if csvSafe(s) != "'"+s {
			t.Fatal("spreadsheet formula not escaped")
		}
	}
	value := "123"
	if csvSafe(&value) != "123" || csvSafe(nil) != "" {
		t.Fatal("invalid CSV scalar")
	}
}
