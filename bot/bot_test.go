package bot

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestFormatDraftText(t *testing.T) {
	prefix := "🖥 myserver $ echo hello"
	shortOutput := "hello"
	formatted := formatDraftText(prefix, shortOutput)
	if formatted != prefix+"\n\nhello" {
		t.Fatalf("unexpected formatted output for short text: %q", formatted)
	}

	// Huge output exceeding 4000 characters
	hugeOutput := strings.Repeat("A", 10000)
	formatted = formatDraftText(prefix, hugeOutput)
	if len(formatted) > 4096 {
		t.Fatalf("formatted length %d exceeds Telegram max 4096", len(formatted))
	}
	if !strings.Contains(formatted, "...") {
		t.Fatalf("expected truncation ellipsis in output")
	}

	// Multi-byte UTF-8 test (Persian / emojis)
	emojiOutput := strings.Repeat("سلام دنیای قشنگ 🌍 ", 500)
	formatted = formatDraftText(prefix, emojiOutput)
	if len(formatted) > 4096 {
		t.Fatalf("formatted length %d exceeds Telegram max 4096 for multi-byte UTF-8", len(formatted))
	}
	if !utf8.ValidString(formatted) {
		t.Fatalf("formatted output is not valid UTF-8: %q", formatted)
	}
}

func TestIsDangerousCommand(t *testing.T) {
	dangerous := []string{
		"rm -rf /",
		"rm -fr /",
		"mkfs.ext4 /dev/sda",
		"dd if=/dev/zero of=/dev/sda",
		":(){ :|:& };:",
		"reboot",
		"shutdown -h now",
		"poweroff",
		"init 0",
		"init 6",
		"> /dev/sda",
		"chmod -R 777 /",
	}

	for _, cmd := range dangerous {
		if !isDangerousCommand(cmd) {
			t.Errorf("expected isDangerousCommand(%q) to be true", cmd)
		}
	}

	safe := []string{
		"ls -la",
		"pwd",
		"cat /etc/os-release",
		"docker ps",
		"systemctl status nginx",
	}

	for _, cmd := range safe {
		if isDangerousCommand(cmd) {
			t.Errorf("expected isDangerousCommand(%q) to be false", cmd)
		}
	}
}
