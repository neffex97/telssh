package bot

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/mymmrac/telego"
	th "github.com/mymmrac/telego/telegohandler"
	tu "github.com/mymmrac/telego/telegoutil"

	"telssh/config"
	"telssh/sshmanager"
)

type runEntry struct {
	id     int64
	cancel context.CancelFunc
}

type dangerEntry struct {
	cmd    string
	isLive bool
}

type editSession struct {
	path      string
	messageID int
}

// Bot wraps the Telegram bot with SSH management.
type Bot struct {
	bot    *telego.Bot
	bh     *th.BotHandler
	cfg    *config.Config
	ssh    *sshmanager.Manager
	cancel context.CancelFunc // cancels long-polling context

	shutdown chan struct{}  // closed when Stop is called
	wg       sync.WaitGroup // tracks background goroutines

	// Running /run commands that can be cancelled
	runMu   sync.Mutex
	runCxls map[int64]runEntry // userID -> runEntry

	// Edit state: tracks users currently in /edit flow
	editMu       sync.Mutex
	editSessions map[int64]editSession // userID -> edit session

	// Upload state: tracks users awaiting file upload
	uploadMu    sync.Mutex
	uploadPaths map[int64]string // userID -> remote path for incoming upload

	// Dangerous command confirmation
	dangerMu   sync.Mutex
	dangerCmds map[int64]dangerEntry // userID -> pending dangerous command

	// Command history (in-memory)
	historyMu sync.Mutex
	history   map[int64][]string // userID -> last N commands

	// Edit server state: tracks users mid-edit
	editServerMu    sync.Mutex
	editServerState map[int64]*editServerSession // userID -> session

	// Draft message streaming
	draftSeq atomic.Int64
}

// editServerSession holds the in-progress server edit.
type editServerSession struct {
	ServerName string // name of server being edited
	Field      string // field user is about to change ("name","host","port","user","password","key_path")
}

// New creates and configures a new Bot.
func New(cfg *config.Config) (*Bot, error) {
	tb, err := telego.NewBot(cfg.BotToken)
	if err != nil {
		return nil, fmt.Errorf("create bot: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	updates, err := tb.UpdatesViaLongPolling(ctx, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("start long polling: %w", err)
	}

	bh, err := th.NewBotHandler(tb, updates)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create handler: %w", err)
	}

	b := &Bot{
		bot:             tb,
		bh:              bh,
		cfg:             cfg,
		ssh:             sshmanager.NewManager(),
		cancel:          cancel,
		shutdown:        make(chan struct{}),
		runCxls:         make(map[int64]runEntry),
		editSessions:    make(map[int64]editSession),
		uploadPaths:     make(map[int64]string),
		dangerCmds:      make(map[int64]dangerEntry),
		history:         make(map[int64][]string),
		editServerState: make(map[int64]*editServerSession),
	}

	b.registerHandlers()
	b.setCommands()
	return b, nil
}

// Start begins handling updates (blocking).
func (b *Bot) Start() {
	log.Println("Bot started. Listening for commands...")
	if err := b.bh.Start(); err != nil {
		log.Printf("Bot handler error: %v", err)
	}
}

// Stop gracefully stops the bot.
func (b *Bot) Stop() {
	close(b.shutdown)   // signal background goroutines
	b.cancel()          // stop long polling
	_ = b.bh.Stop()     // stop handler
	b.cancelAllRuns()   // cancel all running commands
	b.ssh.StopAllLive() // cancel all live sessions
	b.wg.Wait()         // wait for background goroutines to finish
	b.ssh.CloseAll()    // close all SSH connections
}

// cancelAllRuns cancels all active /run command goroutines.
func (b *Bot) cancelAllRuns() {
	b.runMu.Lock()
	defer b.runMu.Unlock()
	for uid, entry := range b.runCxls {
		entry.cancel()
		delete(b.runCxls, uid)
	}
}

// setCommands registers bot commands with Telegram for the command menu.
func (b *Bot) setCommands() {
	commands := []telego.BotCommand{
		{Command: "start", Description: "Start the bot"},
		{Command: "help", Description: "Show all commands"},
		{Command: "servers", Description: "List all servers"},
		{Command: "connect", Description: "Connect to a server"},
		{Command: "disconnect", Description: "Disconnect from server"},
		{Command: "status", Description: "Connection status"},
		{Command: "run", Description: "Run a command"},
		{Command: "live", Description: "Run with live output"},
		{Command: "edit", Description: "Edit a remote file"},
		{Command: "write", Description: "Write content to file"},
		{Command: "download", Description: "Download file from server"},
		{Command: "upload", Description: "Upload file to server"},
		{Command: "quick", Description: "Quick server commands"},
		{Command: "save", Description: "Save a command snippet"},
		{Command: "saved", Description: "Run saved commands"},
		{Command: "delsave", Description: "Delete a saved command"},
		{Command: "history", Description: "Command history"},
		{Command: "shell", Description: "Open persistent shell session"},
		{Command: "closeshell", Description: "Close persistent shell"},
		{Command: "addserver", Description: "Add a new server"},
		{Command: "editserver", Description: "Edit an existing server"},
		{Command: "removeserver", Description: "Remove a server"},
		{Command: "cancel", Description: "Cancel current operation"},
	}
	err := b.bot.SetMyCommands(context.Background(), &telego.SetMyCommandsParams{
		Commands: commands,
	})
	if err != nil {
		log.Printf("Failed to set bot commands: %v", err)
	}
}

// --- helpers ---

func (b *Bot) send(ctx context.Context, chatID int64, text string) error {
	_, err := b.bot.SendMessage(ctx,
		tu.Message(tu.ID(chatID), text).
			WithParseMode(telego.ModeHTML))
	return err
}

func (b *Bot) sendMarkup(ctx context.Context, chatID int64, text string, markup *telego.InlineKeyboardMarkup) error {
	_, err := b.bot.SendMessage(ctx,
		tu.Message(tu.ID(chatID), text).
			WithParseMode(telego.ModeHTML).
			WithReplyMarkup(markup))
	return err
}

func (b *Bot) answer(ctx context.Context, queryID string) error {
	return b.bot.AnswerCallbackQuery(ctx, tu.CallbackQuery(queryID))
}

func (b *Bot) answerText(ctx context.Context, queryID, text string) error {
	return b.bot.AnswerCallbackQuery(ctx, &telego.AnswerCallbackQueryParams{
		CallbackQueryID: queryID,
		Text:            text,
	})
}

func (b *Bot) typing(ctx context.Context, chatID int64) {
	_ = b.bot.SendChatAction(ctx, &telego.SendChatActionParams{
		ChatID: tu.ID(chatID),
		Action: telego.ChatActionTyping,
	})
}

// commandPayload extracts text after /command from message text.
func commandPayload(msg telego.Message) string {
	text := msg.Text
	if text == "" {
		return ""
	}
	// skip the /command[@botname] part
	i := strings.IndexByte(text, ' ')
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(text[i+1:])
}

// userIDFromUpdate extracts user ID from message or callback query.
func userIDFromUpdate(update telego.Update) int64 {
	if update.Message != nil && update.Message.From != nil {
		return update.Message.From.ID
	}
	if update.CallbackQuery != nil {
		return update.CallbackQuery.From.ID
	}
	return 0
}

// chatIDFromUpdate extracts chat ID from message or callback query.
func chatIDFromUpdate(update telego.Update) int64 {
	if update.Message != nil {
		return update.Message.Chat.ID
	}
	if update.CallbackQuery != nil && update.CallbackQuery.Message != nil {
		return update.CallbackQuery.Message.GetChat().ID
	}
	return 0
}

// chatIDFromCallback safely extracts chat ID from a callback query.
func chatIDFromCallback(query telego.CallbackQuery) int64 {
	if query.Message != nil {
		return query.Message.GetChat().ID
	}
	return query.From.ID // fallback for inline mode
}

// --- handler registration ---

func (b *Bot) registerHandlers() {
	// Auth middleware: runs for every update
	b.bh.Use(func(ctx *th.Context, update telego.Update) error {
		uid := userIDFromUpdate(update)
		if uid == 0 {
			return nil // skip updates with no user
		}
		if !b.cfg.IsAuthorized(uid) {
			cid := chatIDFromUpdate(update)
			if cid != 0 {
				_ = b.send(ctx,
					cid,
					"⛔ <b>Unauthorized</b>\n\nYour ID: <code>"+fmt.Sprintf("%d", uid)+"</code>\nAdd this ID to <code>authorized_users</code> in config.yaml",
				)
			}
			return nil // don't propagate
		}
		return ctx.Next(update)
	})

	// Command handlers
	b.bh.HandleMessage(b.handleStart, th.CommandEqual("start"))
	b.bh.HandleMessage(b.handleHelp, th.CommandEqual("help"))
	b.bh.HandleMessage(b.handleServers, th.CommandEqual("servers"))
	b.bh.HandleMessage(b.handleConnect, th.CommandEqual("connect"))
	b.bh.HandleMessage(b.handleDisconnect, th.CommandEqual("disconnect"))
	b.bh.HandleMessage(b.handleStatus, th.CommandEqual("status"))
	b.bh.HandleMessage(b.handleRun, th.CommandEqual("run"))
	b.bh.HandleMessage(b.handleAddServer, th.CommandEqual("addserver"))
	b.bh.HandleMessage(b.handleEditServer, th.CommandEqual("editserver"))
	b.bh.HandleMessage(b.handleRemoveServer, th.CommandEqual("removeserver"))
	b.bh.HandleMessage(b.handleQuick, th.CommandEqual("quick"))
	b.bh.HandleMessage(b.handleLive, th.CommandEqual("live"))
	b.bh.HandleMessage(b.handleEdit, th.CommandEqual("edit"))
	b.bh.HandleMessage(b.handleWrite, th.CommandEqual("write"))
	b.bh.HandleMessage(b.handleDownload, th.CommandEqual("download"))
	b.bh.HandleMessage(b.handleUpload, th.CommandEqual("upload"))
	b.bh.HandleMessage(b.handleSave, th.CommandEqual("save"))
	b.bh.HandleMessage(b.handleSaved, th.CommandEqual("saved"))
	b.bh.HandleMessage(b.handleDelSave, th.CommandEqual("delsave"))
	b.bh.HandleMessage(b.handleHistory, th.CommandEqual("history"))
	b.bh.HandleMessage(b.handleShell, th.CommandEqual("shell"))
	b.bh.HandleMessage(b.handleCloseShell, th.CommandEqual("closeshell"))
	b.bh.HandleMessage(b.handleCancel, th.CommandEqual("cancel"))

	// Callback handlers
	b.bh.HandleCallbackQuery(b.handleConnectCallback, th.CallbackDataPrefix("connect:"))
	b.bh.HandleCallbackQuery(b.handleDisconnectCallback, th.CallbackDataEqual("disconnect"))
	b.bh.HandleCallbackQuery(b.handleQuickCallback, th.CallbackDataPrefix("quick:"))
	b.bh.HandleCallbackQuery(b.handleConfirmRemoveCallback, th.CallbackDataPrefix("confirm_remove:"))
	b.bh.HandleCallbackQuery(b.handleEditServerSelectCallback, th.CallbackDataPrefix("editsrv:"))
	b.bh.HandleCallbackQuery(b.handleEditServerFieldCallback, th.CallbackDataPrefix("editfld:"))
	b.bh.HandleCallbackQuery(b.handleStopLiveCallback, th.CallbackDataEqual("stop_live"))
	b.bh.HandleCallbackQuery(b.handleCancelRunCallback, th.CallbackDataEqual("cancel_run"))
	b.bh.HandleCallbackQuery(b.handleConfirmDangerousCallback, th.CallbackDataPrefix("confirm_danger:"))
	b.bh.HandleCallbackQuery(b.handleCancelDangerousCallback, th.CallbackDataEqual("cancel_danger"))
	b.bh.HandleCallbackQuery(b.handleHistoryCallback, th.CallbackDataPrefix("hist:"))
	b.bh.HandleCallbackQuery(b.handleSavedCallback, th.CallbackDataPrefix("saved:"))
	b.bh.HandleCallbackQuery(b.handleDelSaveCallback, th.CallbackDataPrefix("delsave:"))

	// Document handler (for file uploads)
	b.bh.HandleMessage(b.handleDocument, func(_ context.Context, update telego.Update) bool {
		return update.Message != nil && update.Message.Document != nil
	})

	// Plain text handler (non-command messages)
	b.bh.HandleMessage(b.handleText, th.AnyMessageWithText())
}

// --- message handlers ---

func (b *Bot) handleStart(ctx *th.Context, msg telego.Message) error {
	text := "🖥 <b>TelSSH Bot</b>\n\n" +
		"Manage your VPS servers directly from Telegram.\n\n" +
		"<b>Quick Start:</b>\n" +
		"1️⃣ Add a server → /addserver\n" +
		"2️⃣ Connect → /connect\n" +
		"3️⃣ Type any command to run it\n\n" +
		"Type /help for all commands."
	return b.send(ctx, msg.Chat.ID, text)
}

func (b *Bot) handleHelp(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	active := b.ssh.ActiveServer(uid)
	statusLine := "🔴 Not connected"
	if active != "" {
		statusLine = fmt.Sprintf("🟢 Connected to <b>%s</b>", escapeHTML(active))
	}

	text := fmt.Sprintf(
		"📖 <b>Commands</b>\n\n"+
			"<b>Connection:</b>\n"+
			"/servers — List all servers\n"+
			"/connect — Connect to a server\n"+
			"/disconnect — Disconnect\n"+
			"/status — Connection status\n\n"+
			"<b>Execution:</b>\n"+
			"/run <code>cmd</code> — Run with live streaming + ✋ Cancel\n"+
			"/live <code>cmd</code> — Long-running live output 🔴\n"+
			"Or just type any command directly\n\n"+
			"<b>Files:</b>\n"+
			"/edit <code>path</code> — Read file, reply to update\n"+
			"/write <code>path content</code> — Write content to file\n"+
			"/download <code>path</code> — Download file from server\n"+
			"/upload <code>path</code> — Upload file to server\n\n"+
			"<b>Snippets &amp; History:</b>\n"+
			"/save <code>name cmd</code> — Save a command snippet\n"+
			"/saved — List &amp; run saved commands\n"+
			"/delsave — Delete a saved command\n"+
			"/history — Recent command history\n\n"+
			"<b>Shell:</b>\n"+
			"/shell — Open persistent shell (keeps cd, env, etc.)\n"+
			"/closeshell — Close persistent shell\n\n"+
			"<b>Management:</b>\n"+
			"/addserver — Add a server\n"+
			"/removeserver — Remove a server\n\n"+
			"<b>Quick:</b>\n"+
			"/quick — Common server commands\n\n"+
			"⚡ All commands stream output in real-time via Telegram drafts.\n\n"+
			"<b>Status:</b> %s", statusLine)
	return b.send(ctx, msg.Chat.ID, text)
}

func (b *Bot) handleServers(ctx *th.Context, msg telego.Message) error {
	b.cfg.Mu.RLock()
	servers := make([]config.VPS, len(b.cfg.Servers))
	copy(servers, b.cfg.Servers)
	b.cfg.Mu.RUnlock()

	chatID := msg.Chat.ID
	uid := msg.From.ID

	if len(servers) == 0 {
		return b.send(ctx, chatID, "📭 No servers configured.\nUse /addserver to add one.")
	}

	active := b.ssh.ActiveServer(uid)
	var sb strings.Builder
	sb.WriteString("🗄 <b>Your Servers:</b>\n\n")
	for _, s := range servers {
		indicator := "  ○"
		if s.Name == active {
			indicator = "  🟢"
		}
		authType := "🔑 Key"
		if s.KeyPath == "" {
			authType = "🔒 Pass"
		}
		sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n     <code>%s@%s:%d</code> │ %s\n\n",
			indicator, escapeHTML(s.Name), escapeHTML(s.User), escapeHTML(s.Host), s.Port, authType))
	}
	sb.WriteString("Use /connect to connect.")
	return b.send(ctx, chatID, sb.String())
}

func (b *Bot) handleConnect(ctx *th.Context, msg telego.Message) error {
	b.cfg.Mu.RLock()
	servers := make([]config.VPS, len(b.cfg.Servers))
	copy(servers, b.cfg.Servers)
	b.cfg.Mu.RUnlock()

	chatID := msg.Chat.ID
	uid := msg.From.ID

	if len(servers) == 0 {
		return b.send(ctx, chatID, "📭 No servers configured.\nUse /addserver to add one.")
	}

	args := commandPayload(msg)
	if args != "" {
		return b.connectToServer(ctx, chatID, uid, args)
	}

	var rows [][]telego.InlineKeyboardButton
	for _, s := range servers {
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(fmt.Sprintf("🖥 %s (%s)", s.Name, s.Host)).
				WithCallbackData("connect:"+s.Name),
		))
	}
	return b.sendMarkup(ctx, chatID,
		"🔌 <b>Select a server:</b>",
		tu.InlineKeyboard(rows...))
}

func (b *Bot) handleConnectCallback(ctx *th.Context, query telego.CallbackQuery) error {
	serverName := strings.TrimPrefix(query.Data, "connect:")
	_ = b.answer(ctx, query.ID)
	chatID := chatIDFromCallback(query)
	return b.connectToServer(ctx, chatID, query.From.ID, serverName)
}

func (b *Bot) connectToServer(ctx context.Context, chatID, userID int64, name string) error {
	v, ok := b.cfg.GetServer(name)
	if !ok {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Server <b>%s</b> not found.", escapeHTML(name)))
	}

	_ = b.send(ctx, chatID, fmt.Sprintf(
		"⏳ Connecting to <b>%s</b> (<code>%s@%s:%d</code>)...",
		escapeHTML(v.Name), escapeHTML(v.User), escapeHTML(v.Host), v.Port))

	if err := b.ssh.Connect(userID, v); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ <b>Connection failed:</b>\n<code>%s</code>", escapeHTML(err.Error())))
	}

	return b.send(ctx, chatID, fmt.Sprintf(
		"🟢 Connected to <b>%s</b>!\nType commands directly or use /run <code>cmd</code>",
		escapeHTML(v.Name)))
}

func (b *Bot) handleDisconnect(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID
	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "ℹ️ Not connected to any server.")
	}
	active := b.ssh.ActiveServer(uid)
	b.ssh.Disconnect(uid)
	return b.send(ctx, chatID,
		fmt.Sprintf("🔴 Disconnected from <b>%s</b>.", escapeHTML(active)))
}

func (b *Bot) handleDisconnectCallback(ctx *th.Context, query telego.CallbackQuery) error {
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)
	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "ℹ️ Not connected to any server.")
	}
	active := b.ssh.ActiveServer(uid)
	b.ssh.Disconnect(uid)
	return b.send(ctx, chatID,
		fmt.Sprintf("🔴 Disconnected from <b>%s</b>.", escapeHTML(active)))
}

func (b *Bot) handleStatus(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "🔴 <b>Status:</b> Not connected\n\nUse /connect to connect.")
	}

	active := b.ssh.ActiveServer(uid)
	v, _ := b.cfg.GetServer(active)

	shellStatus := "off"
	if b.ssh.HasShell(uid) {
		shellStatus = "🐚 active"
	}

	text := fmt.Sprintf(
		"🟢 <b>Status:</b> Connected\n\n"+
			"<b>Server:</b> %s\n"+
			"<b>Host:</b> <code>%s:%d</code>\n"+
			"<b>User:</b> <code>%s</code>\n"+
			"<b>Shell:</b> %s\n\n"+
			"Type any command to run it.",
		escapeHTML(v.Name), escapeHTML(v.Host), v.Port, escapeHTML(v.User), shellStatus)

	markup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("⚡ Quick Commands").WithCallbackData("quick:menu"),
			tu.InlineKeyboardButton("🔌 Disconnect").WithCallbackData("disconnect"),
		),
	)
	return b.sendMarkup(ctx, chatID, text, markup)
}

func (b *Bot) handleRun(ctx *th.Context, msg telego.Message) error {
	cmd := commandPayload(msg)
	if cmd == "" {
		return b.send(ctx, msg.Chat.ID,
			"Usage: /run <code>command</code>\nExample: /run <code>ls -la</code>")
	}
	return b.executeCommand(ctx, msg.Chat.ID, msg.From.ID, cmd)
}

func (b *Bot) handleText(ctx *th.Context, msg telego.Message) error {
	text := strings.TrimSpace(msg.Text)
	if text == "" || strings.HasPrefix(text, "/") {
		return nil
	}

	uid := msg.From.ID
	chatID := msg.Chat.ID

	// Check if user is in an /edit flow
	b.editMu.Lock()
	editSess, inEdit := b.editSessions[uid]
	b.editMu.Unlock()

	if inEdit {
		// Require user to reply specifically to the edit prompt message to prevent accidental file overwrites
		if msg.ReplyToMessage != nil && (editSess.messageID == 0 || msg.ReplyToMessage.MessageID == editSess.messageID) {
			b.editMu.Lock()
			delete(b.editSessions, uid)
			b.editMu.Unlock()
			return b.saveEditedFile(ctx, chatID, uid, editSess.path, text)
		}
		return b.send(ctx, chatID, fmt.Sprintf(
			"⚠️ You have an active edit session for <code>%s</code>.\n\n"+
				"Please <b>reply</b> to the file content message with your new content to save, or type /cancel to abort.",
			escapeHTML(editSess.path)))
	}

	// Check if user is in an /editserver flow
	b.editServerMu.Lock()
	eSession, inEditServer := b.editServerState[uid]
	if inEditServer && eSession.Field != "" {
		delete(b.editServerState, uid)
		b.editServerMu.Unlock()
		if eSession.Field == "password" {
			_ = b.bot.DeleteMessage(ctx, &telego.DeleteMessageParams{
				ChatID:    tu.ID(chatID),
				MessageID: msg.MessageID,
			})
		}
		return b.applyEditServerField(ctx, chatID, eSession, text)
	}
	b.editServerMu.Unlock()

	// Check if user is in an /upload flow (sent text instead of file)
	b.uploadMu.Lock()
	_, inUpload := b.uploadPaths[uid]
	b.uploadMu.Unlock()
	if inUpload {
		return b.send(ctx, chatID, "📎 Please send a <b>file</b> (document), not text.\nType /cancel to abort.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}
	return b.executeCommand(ctx, chatID, uid, text)
}

func formatDraftText(prefix, output string) string {
	full := prefix + "\n\n" + output
	if len(full) <= 4000 {
		return full
	}
	avail := 4000 - len(prefix) - 7
	if avail < 200 {
		if len(prefix) > 200 {
			end := 197
			for end > 0 && !utf8.RuneStart(prefix[end]) {
				end--
			}
			prefix = prefix[:end] + "..."
		}
		avail = 4000 - len(prefix) - 7
	}
	if len(output) <= avail {
		return prefix + "\n\n" + output
	}
	start := len(output) - avail
	for start < len(output) && !utf8.RuneStart(output[start]) {
		start++
	}
	res := prefix + "\n\n...\n" + output[start:]
	if len(res) > 4096 {
		res = res[len(res)-4096:]
		for len(res) > 0 && !utf8.RuneStart(res[0]) {
			res = res[1:]
		}
	}
	return res
}

func (b *Bot) executeCommand(ctx context.Context, chatID, userID int64, cmd string) error {
	if !b.ssh.IsConnected(userID) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	// Check dangerous command
	if isDangerousCommand(cmd) {
		return b.confirmDangerousCommand(ctx, chatID, userID, cmd, false)
	}

	// Track in history
	b.addHistory(userID, cmd)

	// If persistent shell is active, run through it (synchronous, simpler)
	if b.ssh.HasShell(userID) {
		return b.executeInShell(ctx, chatID, userID, cmd)
	}

	active := b.ssh.ActiveServer(userID)
	header := fmt.Sprintf("🖥 <b>%s</b> $ <code>%s</code>", escapeHTML(active), escapeHTML(cmd))

	// Send a control message with cancel button
	cancelMarkup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("✋ Cancel").WithCallbackData("cancel_run"),
		),
	)
	ctrlMsg, err := b.bot.SendMessage(ctx,
		tu.Message(tu.ID(chatID), header+"\n<i>⏳ Running...</i>").
			WithParseMode(telego.ModeHTML).
			WithReplyMarkup(cancelMarkup))
	if err != nil {
		return err
	}

	// Set up cancellable context
	runCtx, cancel := context.WithCancel(context.Background())
	runID := b.draftSeq.Add(1)
	b.runMu.Lock()
	if prev, exists := b.runCxls[userID]; exists {
		prev.cancel()
	}
	b.runCxls[userID] = runEntry{id: runID, cancel: cancel}
	b.runMu.Unlock()

	draftID := int(runID)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("executeCommand panic: %v", r)
			}
			b.runMu.Lock()
			if entry, ok := b.runCxls[userID]; ok && entry.id == runID {
				delete(b.runCxls, userID)
			}
			b.runMu.Unlock()
			cancel()
		}()

		bgCtx := context.Background()
		emptyKb := &telego.InlineKeyboardMarkup{InlineKeyboard: [][]telego.InlineKeyboardButton{}}
		var lastOutput string
		var lastDraft time.Time

		streamErr := b.ssh.ExecStreamRun(runCtx, userID, cmd, b.cfg.MaxOutput, 500*time.Millisecond, 2*time.Minute,
			func(output string, final bool) bool {
				if final {
					lastOutput = output
					return true
				}
				// Stream via sendMessageDraft for real-time output
				if output != "" && output != lastOutput && time.Since(lastDraft) > 200*time.Millisecond {
					draftText := formatDraftText("🖥 "+active+" $ "+cmd, output)
					_ = b.bot.SendMessageDraft(bgCtx, &telego.SendMessageDraftParams{
						ChatID:  chatID,
						DraftID: draftID,
						Text:    draftText,
					})
					lastOutput = output
					lastDraft = time.Now()
				}
				return true
			})

		// Edit control message to remove cancel button
		_, _ = b.bot.EditMessageText(bgCtx,
			tu.EditMessageText(tu.ID(chatID), ctrlMsg.MessageID, header).
				WithParseMode(telego.ModeHTML).
				WithReplyMarkup(emptyKb))

		output := lastOutput
		if streamErr != nil && output == "" {
			_ = b.send(bgCtx, chatID, "❌ <pre>"+escapeHTML(streamErr.Error())+"</pre>")
			return
		}

		if output == "" {
			output = "(no output)"
		}

		// Send final output as real message (clears the draft)
		escapedOutput := escapeHTML(output)

		if len(header)+len(escapedOutput)+20 <= 4096 {
			_ = b.send(bgCtx, chatID, "<pre>"+escapedOutput+"</pre>")
		} else if len(escapedOutput)+60 <= 4096 {
			_ = b.send(bgCtx, chatID, "<blockquote expandable><pre>"+escapedOutput+"</pre></blockquote>")
		} else {
			chunks := splitOutput(output, 3900)
			for i, chunk := range chunks {
				esc := escapeHTML(chunk)
				partLabel := ""
				if len(chunks) > 1 {
					partLabel = fmt.Sprintf("📄 Part %d/%d\n", i+1, len(chunks))
				}
				_ = b.send(bgCtx, chatID, partLabel+"<blockquote expandable><pre>"+esc+"</pre></blockquote>")
			}
		}
	}()

	return nil
}

// executeInShell runs a command in the persistent shell session.
func (b *Bot) executeInShell(ctx context.Context, chatID, userID int64, cmd string) error {
	active := b.ssh.ActiveServer(userID)
	header := fmt.Sprintf("🐚 <b>%s</b> $ <code>%s</code>", escapeHTML(active), escapeHTML(cmd))

	b.typing(ctx, chatID)

	output, err := b.ssh.ExecInShell(userID, cmd, b.cfg.MaxOutput)
	if err != nil {
		// If shell died, notify and close it
		if !b.ssh.HasShell(userID) {
			return b.send(ctx, chatID, header+"\n\n❌ Shell session ended: <code>"+escapeHTML(err.Error())+"</code>\nUse /shell to open a new one.")
		}
		return b.send(ctx, chatID, header+"\n\n❌ <pre>"+escapeHTML(err.Error())+"</pre>")
	}

	if output == "" {
		output = "(no output)"
	}

	escapedOutput := escapeHTML(output)

	if len(header)+len(escapedOutput)+20 <= 4096 {
		return b.send(ctx, chatID, header+"\n<pre>"+escapedOutput+"</pre>")
	} else if len(escapedOutput)+60 <= 4096 {
		return b.send(ctx, chatID, header+"\n<blockquote expandable><pre>"+escapedOutput+"</pre></blockquote>")
	} else {
		_ = b.send(ctx, chatID, header)
		chunks := splitOutput(output, 3900)
		for i, chunk := range chunks {
			esc := escapeHTML(chunk)
			partLabel := ""
			if len(chunks) > 1 {
				partLabel = fmt.Sprintf("📄 Part %d/%d\n", i+1, len(chunks))
			}
			_ = b.send(ctx, chatID, partLabel+"<blockquote expandable><pre>"+esc+"</pre></blockquote>")
		}
		return nil
	}
}

func (b *Bot) handleCancelRunCallback(ctx *th.Context, query telego.CallbackQuery) error {
	_ = b.answerText(ctx, query.ID, "✋ Cancelling...")
	uid := query.From.ID
	b.runMu.Lock()
	if entry, ok := b.runCxls[uid]; ok {
		entry.cancel()
	}
	b.runMu.Unlock()
	return nil
}

// --- Live Streaming ---

func (b *Bot) handleLive(ctx *th.Context, msg telego.Message) error {
	cmd := commandPayload(msg)
	chatID := msg.Chat.ID
	uid := msg.From.ID

	if cmd == "" {
		return b.send(ctx, chatID,
			"🔴 <b>Live Mode</b>\n\n"+
				"Stream command output in real-time via <b>sendMessageDraft</b>.\n\n"+
				"<b>Usage:</b> /live <code>command</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/live tail -f /var/log/syslog</code>\n"+
				"<code>/live htop -d 20</code>\n"+
				"<code>/live watch -n2 free -h</code>\n"+
				"<code>/live ping google.com</code>\n"+
				"<code>/live apt update &amp;&amp; apt upgrade -y</code>\n\n"+
				"Output streams in real-time. Max duration: 5 min.\n"+
				"Press ⏹ <b>Stop</b> to cancel anytime.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	if isDangerousCommand(cmd) {
		return b.confirmDangerousCommand(ctx, chatID, uid, cmd, true)
	}

	return b.startLive(ctx, chatID, uid, cmd)
}

func (b *Bot) startLive(ctx context.Context, chatID, uid int64, cmd string) error {
	if b.ssh.HasLive(uid) {
		return b.send(ctx, chatID, "⚠️ A live session is already running. Stop it first.")
	}

	active := b.ssh.ActiveServer(uid)
	b.addHistory(uid, cmd)

	// Send control message with stop button
	stopMarkup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("⏹ Stop").WithCallbackData("stop_live"),
		),
	)

	header := fmt.Sprintf("🔴 <b>LIVE</b> — <b>%s</b> $ <code>%s</code>", escapeHTML(active), escapeHTML(cmd))
	ctrlMsg, err := b.bot.SendMessage(ctx,
		tu.Message(tu.ID(chatID), header+"\n<i>⏳ Starting...</i>").
			WithParseMode(telego.ModeHTML).
			WithReplyMarkup(stopMarkup))
	if err != nil {
		return b.send(ctx, chatID, "❌ Failed to start live session.")
	}

	draftID := int(b.draftSeq.Add(1))

	// Run in background goroutine
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Live stream panic: %v", r)
			}
		}()

		bgCtx := context.Background()
		emptyKb := &telego.InlineKeyboardMarkup{InlineKeyboard: [][]telego.InlineKeyboardButton{}}
		var lastOutput string
		var lastDraft time.Time

		streamErr := b.ssh.ExecStream(bgCtx, uid, cmd, b.cfg.MaxOutput, 1*time.Second, 5*time.Minute,
			func(output string, final bool) bool {
				if final {
					lastOutput = output
					return true
				}

				// Stream via sendMessageDraft for true real-time output
				if output != "" && time.Since(lastDraft) > 300*time.Millisecond {
					draftText := formatDraftText("🔴 LIVE — "+active+" $ "+cmd, output)
					_ = b.bot.SendMessageDraft(bgCtx, &telego.SendMessageDraftParams{
						ChatID:  chatID,
						DraftID: draftID,
						Text:    draftText,
					})
					lastOutput = output
					lastDraft = time.Now()
				}
				return true
			})

		// Edit control message to show finished
		_, _ = b.bot.EditMessageText(bgCtx,
			tu.EditMessageText(tu.ID(chatID), ctrlMsg.MessageID,
				header+"\n✅ <b>Finished</b>").
				WithParseMode(telego.ModeHTML).
				WithReplyMarkup(emptyKb))

		output := lastOutput
		if streamErr != nil {
			log.Printf("Live stream error for user %d: %v", uid, streamErr)
			if output == "" {
				_ = b.send(bgCtx, chatID, "❌ "+escapeHTML(streamErr.Error()))
				return
			}
		}

		if output == "" {
			output = "(no output)"
		}

		// Send final output as real message (clears the draft)
		escaped := escapeHTML(output)
		maxChars := 3800
		if len(escaped) > maxChars {
			escaped = "...(truncated)...\n" + escaped[len(escaped)-maxChars:]
		}
		_ = b.send(bgCtx, chatID, "<pre>"+escaped+"</pre>")
	}()

	return nil
}

func (b *Bot) handleStopLiveCallback(ctx *th.Context, query telego.CallbackQuery) error {
	_ = b.answerText(ctx, query.ID, "⏹ Stopping...")
	b.ssh.StopLive(query.From.ID)
	return nil
}

// --- File Editing ---

func (b *Bot) handleCancel(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID
	cancelled := false

	b.editMu.Lock()
	if _, inEdit := b.editSessions[uid]; inEdit {
		delete(b.editSessions, uid)
		cancelled = true
	}
	b.editMu.Unlock()

	b.uploadMu.Lock()
	if _, inUpload := b.uploadPaths[uid]; inUpload {
		delete(b.uploadPaths, uid)
		cancelled = true
	}
	b.uploadMu.Unlock()

	b.dangerMu.Lock()
	if _, inDanger := b.dangerCmds[uid]; inDanger {
		delete(b.dangerCmds, uid)
		cancelled = true
	}
	b.dangerMu.Unlock()

	b.editServerMu.Lock()
	if _, inES := b.editServerState[uid]; inES {
		delete(b.editServerState, uid)
		cancelled = true
	}
	b.editServerMu.Unlock()

	// Also cancel running /run command if any
	b.runMu.Lock()
	if entry, ok := b.runCxls[uid]; ok {
		entry.cancel()
		delete(b.runCxls, uid)
		cancelled = true
	}
	b.runMu.Unlock()

	// Cancel running live session if any
	if b.ssh.HasLive(uid) {
		b.ssh.StopLive(uid)
		cancelled = true
	}

	if cancelled {
		return b.send(ctx, chatID, "🚫 Cancelled.")
	}
	return b.send(ctx, chatID, "👍 Nothing to cancel.")
}

func (b *Bot) handleEdit(ctx *th.Context, msg telego.Message) error {
	path := commandPayload(msg)
	chatID := msg.Chat.ID
	uid := msg.From.ID

	if path == "" {
		return b.send(ctx, chatID,
			"📝 <b>Edit File</b>\n\n"+
				"<b>Usage:</b> /edit <code>path</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/edit /etc/nginx/nginx.conf</code>\n"+
				"<code>/edit ~/.bashrc</code>\n\n"+
				"The bot will show the file content.\n"+
				"Send your updated content as a reply to save it.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	b.typing(ctx, chatID)

	// Read file up to 8000 bytes for editing in chat
	rawBytes, _, err := b.ssh.DownloadFile(uid, path, 8000)
	if err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ Cannot read <code>%s</code>:\n<pre>%s</pre>",
			escapeHTML(path), escapeHTML(err.Error())))
	}

	if len(rawBytes) >= 8000 {
		return b.send(ctx, chatID, fmt.Sprintf(
			"⚠️ File <code>%s</code> is too large to edit via Telegram chat (>8KB).\n\nPlease use /download and /upload instead.",
			escapeHTML(path)))
	}

	if !utf8.Valid(rawBytes) {
		return b.send(ctx, chatID, fmt.Sprintf(
			"⚠️ File <code>%s</code> contains binary data or invalid UTF-8 characters and cannot be edited in chat.\n\nPlease use /download and /upload instead.",
			escapeHTML(path)))
	}

	content := sshmanager.StripAnsi(string(rawBytes))

	escaped := escapeHTML(content)
	header := fmt.Sprintf("📝 <b>Editing:</b> <code>%s</code>\n\n", escapeHTML(path))
	footer := "\n\n💬 <b>Reply to this message with your updated content to save.</b>\n" +
		"Type /cancel to abort."

	var promptMsg *telego.Message
	if len(header)+len(escaped)+len(footer)+20 <= 4096 {
		promptMsg, _ = b.bot.SendMessage(ctx, tu.Message(tu.ID(chatID), header+"<pre>"+escaped+"</pre>"+footer).WithParseMode(telego.ModeHTML))
	} else if len(header)+len(escaped)+len(footer)+60 <= 4096 {
		promptMsg, _ = b.bot.SendMessage(ctx, tu.Message(tu.ID(chatID), header+"<blockquote expandable><pre>"+escaped+"</pre></blockquote>"+footer).WithParseMode(telego.ModeHTML))
	} else {
		_ = b.send(ctx, chatID, header)
		chunks := splitOutput(content, 3900)
		for i, chunk := range chunks {
			esc := escapeHTML(chunk)
			label := ""
			if len(chunks) > 1 {
				label = fmt.Sprintf("📄 Part %d/%d\n", i+1, len(chunks))
			}
			_ = b.send(ctx, chatID, label+"<blockquote expandable><pre>"+esc+"</pre></blockquote>")
		}
		promptMsg, _ = b.bot.SendMessage(ctx, tu.Message(tu.ID(chatID), footer).WithParseMode(telego.ModeHTML))
	}

	if promptMsg != nil {
		b.editMu.Lock()
		b.editSessions[uid] = editSession{path: path, messageID: promptMsg.MessageID}
		b.editMu.Unlock()
	} else {
		return b.send(ctx, chatID, "❌ Failed to display file for editing.")
	}
	return nil
}

func (b *Bot) saveEditedFile(ctx context.Context, chatID, userID int64, path, content string) error {
	if !b.ssh.IsConnected(userID) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	b.typing(ctx, chatID)

	if err := b.ssh.WriteFile(userID, path, content); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ Failed to write <code>%s</code>:\n<pre>%s</pre>",
			escapeHTML(path), escapeHTML(err.Error())))
	}

	return b.send(ctx, chatID, fmt.Sprintf(
		"✅ File saved: <code>%s</code>\n(%d bytes written)",
		escapeHTML(path), len(content)))
}

func (b *Bot) handleWrite(ctx *th.Context, msg telego.Message) error {
	payload := commandPayload(msg)
	chatID := msg.Chat.ID
	uid := msg.From.ID

	if payload == "" {
		return b.send(ctx, chatID,
			"📝 <b>Write File</b>\n\n"+
				"<b>Usage:</b> /write <code>path</code> <code>content</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/write /tmp/hello.txt Hello World!</code>\n"+
				"<code>/write /tmp/test.sh #!/bin/bash\necho hi</code>\n\n"+
				"For editing existing files, use /edit instead.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	// Split: first word = path, rest = content
	parts := strings.SplitN(payload, " ", 2)
	if len(parts) < 2 {
		return b.send(ctx, chatID, "❌ Need both path and content.\nUsage: /write <code>path</code> <code>content</code>")
	}

	path := parts[0]
	content := parts[1]

	b.typing(ctx, chatID)

	if err := b.ssh.WriteFile(uid, path, content); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ Failed to write <code>%s</code>:\n<pre>%s</pre>",
			escapeHTML(path), escapeHTML(err.Error())))
	}

	return b.send(ctx, chatID, fmt.Sprintf(
		"✅ File written: <code>%s</code>\n(%d bytes)",
		escapeHTML(path), len(content)))
}

func (b *Bot) handleAddServer(ctx *th.Context, msg telego.Message) error {
	args := commandPayload(msg)
	chatID := msg.Chat.ID

	// Delete the message containing credentials for security
	_ = b.bot.DeleteMessage(ctx, &telego.DeleteMessageParams{
		ChatID:    tu.ID(chatID),
		MessageID: msg.MessageID,
	})

	if args == "" {
		return b.send(ctx, chatID,
			"➕ <b>Add Server</b>\n\n"+
				"<b>Format:</b>\n"+
				"<code>/addserver name host user password [port] [key_path]</code>\n"+
				"<code>/addserver name host:port user password [key_path]</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/addserver prod 1.2.3.4 root mypass</code>\n"+
				"<code>/addserver prod 1.2.3.4:2222 root mypass</code>\n"+
				"<code>/addserver staging 5.6.7.8 ubuntu \"\" 22 ~/.ssh/id_rsa</code>\n\n"+
				"🔒 Your message will be deleted for security.\n"+
				"Use /editserver to modify an existing server.")
	}

	parts := parseArgs(args)
	if len(parts) < 4 {
		return b.send(ctx, chatID, "❌ Need at least: <code>name host user password</code>\nUse /addserver for help.")
	}

	v := config.VPS{
		Name:     parts[0],
		User:     parts[2],
		Password: parts[3],
		Port:     22,
	}

	// Support host:port format
	hostPart := parts[1]
	if colonIdx := strings.LastIndex(hostPart, ":"); colonIdx > 0 {
		if port, err := strconv.Atoi(hostPart[colonIdx+1:]); err == nil {
			v.Host = hostPart[:colonIdx]
			v.Port = port
		} else {
			v.Host = hostPart
		}
	} else {
		v.Host = hostPart
	}

	if len(parts) == 5 {
		if port, err := strconv.Atoi(parts[4]); err == nil {
			v.Port = port
		} else {
			v.KeyPath = parts[4]
		}
	} else if len(parts) >= 6 {
		if port, err := strconv.Atoi(parts[4]); err == nil {
			v.Port = port
		}
		v.KeyPath = parts[5]
	}

	if _, exists := b.cfg.GetServer(v.Name); exists {
		return b.send(ctx, chatID, fmt.Sprintf(
			"⚠️ Server <b>%s</b> already exists. Remove it first.",
			escapeHTML(v.Name)))
	}

	if err := b.cfg.AddServer(v); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Failed to add server: %s", escapeHTML(err.Error())))
	}

	return b.send(ctx, chatID, fmt.Sprintf(
		"✅ Server <b>%s</b> added!\n<code>%s@%s:%d</code>\n\nUse /connect %s to connect.",
		escapeHTML(v.Name), escapeHTML(v.User), escapeHTML(v.Host), v.Port, escapeHTML(v.Name)))
}

func (b *Bot) handleRemoveServer(ctx *th.Context, msg telego.Message) error {
	b.cfg.Mu.RLock()
	servers := make([]config.VPS, len(b.cfg.Servers))
	copy(servers, b.cfg.Servers)
	b.cfg.Mu.RUnlock()

	chatID := msg.Chat.ID

	if len(servers) == 0 {
		return b.send(ctx, chatID, "📭 No servers to remove.")
	}

	args := commandPayload(msg)
	if args != "" {
		markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("🗑 Yes, remove").WithCallbackData("confirm_remove:" + args),
			),
		)
		return b.sendMarkup(ctx, chatID,
			fmt.Sprintf("⚠️ Remove server <b>%s</b>?", escapeHTML(args)), markup)
	}

	var rows [][]telego.InlineKeyboardButton
	for _, s := range servers {
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(fmt.Sprintf("🗑 %s (%s)", s.Name, s.Host)).
				WithCallbackData("confirm_remove:"+s.Name),
		))
	}
	return b.sendMarkup(ctx, chatID,
		"🗑 <b>Select server to remove:</b>",
		tu.InlineKeyboard(rows...))
}

func (b *Bot) handleConfirmRemoveCallback(ctx *th.Context, query telego.CallbackQuery) error {
	name := strings.TrimPrefix(query.Data, "confirm_remove:")
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	if b.ssh.ActiveServer(uid) == name {
		b.ssh.Disconnect(uid)
	}

	if err := b.cfg.RemoveServer(name); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Error: %s", escapeHTML(err.Error())))
	}
	return b.send(ctx, chatID, fmt.Sprintf("✅ Server <b>%s</b> removed.", escapeHTML(name)))
}

// ─── Edit Server ────────────────────────────────────────────────────────────

func (b *Bot) handleEditServer(ctx *th.Context, msg telego.Message) error {
	b.cfg.Mu.RLock()
	servers := make([]config.VPS, len(b.cfg.Servers))
	copy(servers, b.cfg.Servers)
	b.cfg.Mu.RUnlock()

	chatID := msg.Chat.ID

	if len(servers) == 0 {
		return b.send(ctx, chatID, "📭 No servers configured. Use /addserver first.")
	}

	// If user passed a name directly: /editserver prod
	args := commandPayload(msg)
	if args != "" {
		if _, ok := b.cfg.GetServer(args); !ok {
			return b.send(ctx, chatID, fmt.Sprintf("❌ Server <b>%s</b> not found.", escapeHTML(args)))
		}
		return b.showEditServerFields(ctx, chatID, args)
	}

	// Otherwise, show server selection buttons
	var rows [][]telego.InlineKeyboardButton
	for _, s := range servers {
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(fmt.Sprintf("✏️ %s (%s:%d)", s.Name, s.Host, s.Port)).
				WithCallbackData("editsrv:"+s.Name),
		))
	}
	return b.sendMarkup(ctx, chatID,
		"✏️ <b>Select server to edit:</b>",
		tu.InlineKeyboard(rows...))
}

func (b *Bot) handleEditServerSelectCallback(ctx *th.Context, query telego.CallbackQuery) error {
	name := strings.TrimPrefix(query.Data, "editsrv:")
	_ = b.answer(ctx, query.ID)
	chatID := chatIDFromCallback(query)

	if _, ok := b.cfg.GetServer(name); !ok {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Server <b>%s</b> not found.", escapeHTML(name)))
	}
	return b.showEditServerFields(ctx, chatID, name)
}

func (b *Bot) showEditServerFields(ctx context.Context, chatID int64, serverName string) error {
	srv, _ := b.cfg.GetServer(serverName)

	text := fmt.Sprintf(
		"✏️ <b>Editing:</b> %s\n\n"+
			"<b>Host:</b> <code>%s</code>\n"+
			"<b>Port:</b> <code>%d</code>\n"+
			"<b>User:</b> <code>%s</code>\n"+
			"<b>Password:</b> <code>%s</code>\n"+
			"<b>Key Path:</b> <code>%s</code>\n\n"+
			"Tap a field to change it:",
		escapeHTML(srv.Name),
		escapeHTML(srv.Host),
		srv.Port,
		escapeHTML(srv.User),
		maskPassword(srv.Password),
		escapeHTML(srv.KeyPath),
	)

	rows := [][]telego.InlineKeyboardButton{
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("📝 Name").WithCallbackData("editfld:"+serverName+":name"),
			tu.InlineKeyboardButton("🌐 Host").WithCallbackData("editfld:"+serverName+":host"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🔌 Port").WithCallbackData("editfld:"+serverName+":port"),
			tu.InlineKeyboardButton("👤 User").WithCallbackData("editfld:"+serverName+":user"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🔑 Password").WithCallbackData("editfld:"+serverName+":password"),
			tu.InlineKeyboardButton("📄 Key Path").WithCallbackData("editfld:"+serverName+":key_path"),
		),
	}
	return b.sendMarkup(ctx, chatID, text, tu.InlineKeyboard(rows...))
}

func (b *Bot) handleEditServerFieldCallback(ctx *th.Context, query telego.CallbackQuery) error {
	data := strings.TrimPrefix(query.Data, "editfld:")
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	// data format: "serverName:field"
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 {
		return b.send(ctx, chatID, "❌ Invalid callback data.")
	}
	serverName, field := parts[0], parts[1]

	if _, ok := b.cfg.GetServer(serverName); !ok {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Server <b>%s</b> not found.", escapeHTML(serverName)))
	}

	fieldLabels := map[string]string{
		"name": "Name", "host": "Host", "port": "Port",
		"user": "User", "password": "Password", "key_path": "Key Path",
	}
	label, ok := fieldLabels[field]
	if !ok {
		return b.send(ctx, chatID, "❌ Unknown field.")
	}

	b.editServerMu.Lock()
	b.editServerState[uid] = &editServerSession{
		ServerName: serverName,
		Field:      field,
	}
	b.editServerMu.Unlock()

	return b.send(ctx, chatID,
		fmt.Sprintf("✏️ Send the new <b>%s</b> for server <b>%s</b>:\n\n(Type /cancel to abort)", label, escapeHTML(serverName)))
}

func (b *Bot) applyEditServerField(ctx context.Context, chatID int64, session *editServerSession, value string) error {
	srv, ok := b.cfg.GetServer(session.ServerName)
	if !ok {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Server <b>%s</b> no longer exists.", escapeHTML(session.ServerName)))
	}

	oldName := session.ServerName

	switch session.Field {
	case "name":
		srv.Name = value
	case "host":
		// support host:port
		if colonIdx := strings.LastIndex(value, ":"); colonIdx > 0 {
			if port, err := strconv.Atoi(value[colonIdx+1:]); err == nil {
				srv.Host = value[:colonIdx]
				srv.Port = port
			} else {
				srv.Host = value
			}
		} else {
			srv.Host = value
		}
	case "port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return b.send(ctx, chatID, "❌ Port must be a number.")
		}
		srv.Port = port
	case "user":
		srv.User = value
	case "password":
		srv.Password = value
	case "key_path":
		srv.KeyPath = value
	default:
		return b.send(ctx, chatID, "❌ Unknown field.")
	}

	if err := b.cfg.UpdateServer(oldName, srv); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf("❌ %s", escapeHTML(err.Error())))
	}

	fieldLabels := map[string]string{
		"name": "Name", "host": "Host", "port": "Port",
		"user": "User", "password": "Password", "key_path": "Key Path",
	}
	return b.send(ctx, chatID,
		fmt.Sprintf("✅ <b>%s</b> updated for server <b>%s</b>.",
			fieldLabels[session.Field], escapeHTML(srv.Name)))
}

func maskPassword(pw string) string {
	if pw == "" {
		return "(none)"
	}
	return "••••••"
}

func (b *Bot) handleQuick(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID
	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}
	return b.sendQuickMenu(ctx, chatID, uid)
}

func (b *Bot) handleQuickCallback(ctx *th.Context, query telego.CallbackQuery) error {
	data := strings.TrimPrefix(query.Data, "quick:")
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	if data == "menu" {
		return b.sendQuickMenu(ctx, chatID, uid)
	}

	commands := map[string]string{
		"sysinfo":    "uname -a && echo '---' && uptime",
		"disk":       "df -h",
		"memory":     "free -h",
		"cpu":        "top -bn1 | head -20",
		"processes":  "ps aux --sort=-%mem | head -15",
		"network":    "ss -tulnp",
		"docker_ps":  "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo 'Docker not found'",
		"docker_img": "docker images --format 'table {{.Repository}}\t{{.Tag}}\t{{.Size}}' 2>/dev/null || echo 'Docker not found'",
		"logs":       "journalctl --no-pager -n 30",
		"services":   "systemctl list-units --type=service --state=running --no-pager | head -25",
		"updates":    "apt list --upgradable 2>/dev/null || yum check-update 2>/dev/null || echo 'Unknown pkg manager'",
		"ip":         "curl -s ifconfig.me && echo '' && ip addr show | grep 'inet ' | awk '{print $2}'",
		"load":       "cat /proc/loadavg && echo '---' && vmstat 1 3",
		"users":      "who && echo '---' && last -10",
	}

	cmd, ok := commands[data]
	if !ok {
		return b.send(ctx, chatID, "❌ Unknown quick command.")
	}
	return b.executeCommand(ctx, chatID, uid, cmd)
}

func (b *Bot) sendQuickMenu(ctx context.Context, chatID, userID int64) error {
	active := b.ssh.ActiveServer(userID)
	markup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("📊 System Info").WithCallbackData("quick:sysinfo"),
			tu.InlineKeyboardButton("💾 Disk Usage").WithCallbackData("quick:disk"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🧠 Memory").WithCallbackData("quick:memory"),
			tu.InlineKeyboardButton("⚙️ CPU/Top").WithCallbackData("quick:cpu"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("📋 Processes").WithCallbackData("quick:processes"),
			tu.InlineKeyboardButton("🌐 Network").WithCallbackData("quick:network"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🐳 Docker PS").WithCallbackData("quick:docker_ps"),
			tu.InlineKeyboardButton("📦 Docker Imgs").WithCallbackData("quick:docker_img"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("📜 Sys Logs").WithCallbackData("quick:logs"),
			tu.InlineKeyboardButton("🔧 Services").WithCallbackData("quick:services"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🔄 Updates").WithCallbackData("quick:updates"),
			tu.InlineKeyboardButton("🌍 IP Address").WithCallbackData("quick:ip"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("📈 Load Avg").WithCallbackData("quick:load"),
			tu.InlineKeyboardButton("👥 Users").WithCallbackData("quick:users"),
		),
	)
	return b.sendMarkup(ctx, chatID,
		fmt.Sprintf("⚡ <b>Quick Commands</b> — %s\n\nTap to run:", escapeHTML(active)),
		markup)
}

// --- File Download/Upload ---

func (b *Bot) handleDownload(ctx *th.Context, msg telego.Message) error {
	path := commandPayload(msg)
	chatID := msg.Chat.ID
	uid := msg.From.ID

	if path == "" {
		return b.send(ctx, chatID,
			"📥 <b>Download File</b>\n\n"+
				"<b>Usage:</b> /download <code>path</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/download /etc/nginx/nginx.conf</code>\n"+
				"<code>/download /var/log/syslog</code>\n\n"+
				"Max file size: 50MB (Telegram limit).")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	b.typing(ctx, chatID)

	data, filename, err := b.ssh.DownloadFile(uid, path, 50*1024*1024) // 50MB limit
	if err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ Download failed:\n<pre>%s</pre>", escapeHTML(err.Error())))
	}

	active := b.ssh.ActiveServer(uid)
	caption := fmt.Sprintf("📥 <b>%s</b>:%s\n%d bytes", escapeHTML(active), escapeHTML(path), len(data))

	file := tu.Document(tu.ID(chatID), tu.FileFromBytes(data, filename)).
		WithCaption(caption).WithParseMode(telego.ModeHTML)

	_, err = b.bot.SendDocument(ctx, file)
	if err != nil {
		return b.send(ctx, chatID, "❌ Failed to send file: "+escapeHTML(err.Error()))
	}
	return nil
}

func (b *Bot) handleUpload(ctx *th.Context, msg telego.Message) error {
	path := commandPayload(msg)
	chatID := msg.Chat.ID
	uid := msg.From.ID

	if path == "" {
		return b.send(ctx, chatID,
			"📤 <b>Upload File</b>\n\n"+
				"<b>Usage:</b> /upload <code>remote_path</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/upload /tmp/deploy.sh</code>\n"+
				"<code>/upload /etc/nginx/site.conf</code>\n\n"+
				"After the command, send the file as a document.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	b.uploadMu.Lock()
	b.uploadPaths[uid] = path
	b.uploadMu.Unlock()

	return b.send(ctx, chatID, fmt.Sprintf(
		"📤 Ready to upload to <code>%s</code>\n\n📎 <b>Send the file now</b> (as a document).\nType /cancel to abort.",
		escapeHTML(path)))
}

func (b *Bot) handleDocument(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID
	doc := msg.Document

	if doc == nil {
		return nil
	}

	// Check for pending upload path
	b.uploadMu.Lock()
	remotePath, hasUpload := b.uploadPaths[uid]
	if hasUpload {
		delete(b.uploadPaths, uid)
	}
	b.uploadMu.Unlock()

	// If no pending upload, check if caption contains a path
	if !hasUpload && msg.Caption != "" {
		caption := strings.TrimSpace(msg.Caption)
		if strings.HasPrefix(caption, "/") || strings.HasPrefix(caption, "~") {
			remotePath = caption
			hasUpload = true
		}
	}

	if !hasUpload {
		return b.send(ctx, chatID, "📤 To upload, use /upload <code>path</code> first, or send the file with the remote path as caption.")
	}

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	b.typing(ctx, chatID)

	// Download file from Telegram
	tFile, err := b.bot.GetFile(ctx, &telego.GetFileParams{FileID: doc.FileID})
	if err != nil {
		return b.send(ctx, chatID, "❌ Failed to get file info: "+escapeHTML(err.Error()))
	}

	fileURL := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", b.cfg.BotToken, tFile.FilePath)

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer httpCancel()
	req, _ := http.NewRequestWithContext(httpCtx, "GET", fileURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cleanErr := strings.ReplaceAll(err.Error(), b.cfg.BotToken, "[REDACTED_TOKEN]")
		return b.send(ctx, chatID, "❌ Failed to download from Telegram: "+escapeHTML(cleanErr))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Telegram download failed: HTTP %d", resp.StatusCode))
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024))
	if err != nil {
		return b.send(ctx, chatID, "❌ Failed to read file: "+escapeHTML(err.Error()))
	}

	// Upload to VPS
	if err := b.ssh.UploadFile(uid, remotePath, data); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf(
			"❌ Upload failed:\n<pre>%s</pre>", escapeHTML(err.Error())))
	}

	active := b.ssh.ActiveServer(uid)
	return b.send(ctx, chatID, fmt.Sprintf(
		"✅ Uploaded to <b>%s</b>:<code>%s</code>\n📦 %d bytes (%s)",
		escapeHTML(active), escapeHTML(remotePath), len(data), escapeHTML(doc.FileName)))
}

// --- Saved Commands ---

func (b *Bot) handleSave(ctx *th.Context, msg telego.Message) error {
	payload := commandPayload(msg)
	chatID := msg.Chat.ID

	if payload == "" {
		return b.send(ctx, chatID,
			"💾 <b>Save Command</b>\n\n"+
				"<b>Usage:</b> /save <code>name command</code>\n\n"+
				"<b>Examples:</b>\n"+
				"<code>/save deploy cd /app && git pull && docker compose up -d</code>\n"+
				"<code>/save logs tail -n 100 /var/log/syslog</code>\n"+
				"<code>/save update apt update && apt upgrade -y</code>\n\n"+
				"Use /saved to list and run saved commands.")
	}

	parts := strings.SplitN(payload, " ", 2)
	if len(parts) < 2 {
		return b.send(ctx, chatID, "❌ Need both name and command.\nUsage: /save <code>name command</code>")
	}

	name := parts[0]
	cmd := parts[1]

	if err := b.cfg.AddSavedCommand(name, cmd); err != nil {
		return b.send(ctx, chatID, "❌ Failed to save: "+escapeHTML(err.Error()))
	}

	return b.send(ctx, chatID, fmt.Sprintf(
		"💾 Saved <b>%s</b>:\n<code>%s</code>\n\nUse /saved to run it.",
		escapeHTML(name), escapeHTML(cmd)))
}

func (b *Bot) handleSaved(ctx *th.Context, msg telego.Message) error {
	chatID := msg.Chat.ID
	saved := b.cfg.GetSavedCommands()

	if len(saved) == 0 {
		return b.send(ctx, chatID, "📭 No saved commands.\nUse /save <code>name command</code> to add one.")
	}

	var rows [][]telego.InlineKeyboardButton
	for _, sc := range saved {
		label := fmt.Sprintf("▶️ %s", sc.Name)
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(label).WithCallbackData("saved:"+sc.Name),
		))
	}

	var sb strings.Builder
	sb.WriteString("💾 <b>Saved Commands:</b>\n\n")
	for _, sc := range saved {
		sb.WriteString(fmt.Sprintf("  • <b>%s</b>: <code>%s</code>\n", escapeHTML(sc.Name), escapeHTML(sc.Command)))
	}
	sb.WriteString("\nTap to run:")

	return b.sendMarkup(ctx, chatID, sb.String(), tu.InlineKeyboard(rows...))
}

func (b *Bot) handleSavedCallback(ctx *th.Context, query telego.CallbackQuery) error {
	name := strings.TrimPrefix(query.Data, "saved:")
	_ = b.answer(ctx, query.ID)
	chatID := chatIDFromCallback(query)
	uid := query.From.ID

	cmd, ok := b.cfg.GetSavedCommand(name)
	if !ok {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Snippet <b>%s</b> not found.", escapeHTML(name)))
	}
	return b.executeCommand(ctx, chatID, uid, cmd)
}

func (b *Bot) handleDelSave(ctx *th.Context, msg telego.Message) error {
	chatID := msg.Chat.ID
	saved := b.cfg.GetSavedCommands()

	if len(saved) == 0 {
		return b.send(ctx, chatID, "📭 No saved commands to delete.")
	}

	args := commandPayload(msg)
	if args != "" {
		if err := b.cfg.RemoveSavedCommand(args); err != nil {
			return b.send(ctx, chatID, "❌ "+escapeHTML(err.Error()))
		}
		return b.send(ctx, chatID, fmt.Sprintf("✅ Deleted snippet <b>%s</b>.", escapeHTML(args)))
	}

	var rows [][]telego.InlineKeyboardButton
	for _, sc := range saved {
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🗑 "+sc.Name).WithCallbackData("delsave:"+sc.Name),
		))
	}
	return b.sendMarkup(ctx, chatID, "🗑 <b>Select snippet to delete:</b>", tu.InlineKeyboard(rows...))
}

func (b *Bot) handleDelSaveCallback(ctx *th.Context, query telego.CallbackQuery) error {
	name := strings.TrimPrefix(query.Data, "delsave:")
	_ = b.answer(ctx, query.ID)
	chatID := chatIDFromCallback(query)

	if err := b.cfg.RemoveSavedCommand(name); err != nil {
		return b.send(ctx, chatID, "❌ "+escapeHTML(err.Error()))
	}
	return b.send(ctx, chatID, fmt.Sprintf("✅ Deleted snippet <b>%s</b>.", escapeHTML(name)))
}

// --- Command History ---

const maxHistory = 20

func (b *Bot) addHistory(userID int64, cmd string) {
	b.historyMu.Lock()
	defer b.historyMu.Unlock()
	h := b.history[userID]
	// Don't add consecutive duplicates
	if len(h) > 0 && h[len(h)-1] == cmd {
		return
	}
	h = append(h, cmd)
	if len(h) > maxHistory {
		h = h[len(h)-maxHistory:]
	}
	b.history[userID] = h
}

func (b *Bot) handleHistory(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID

	b.historyMu.Lock()
	h := b.history[uid]
	result := make([]string, len(h))
	copy(result, h)
	b.historyMu.Unlock()

	if len(result) == 0 {
		return b.send(ctx, chatID, "📜 No command history yet.")
	}

	var sb strings.Builder
	sb.WriteString("📜 <b>Command History:</b>\n\n")

	// Show most recent first, with inline buttons
	var rows [][]telego.InlineKeyboardButton
	maxButtons := 10
	start := len(result) - maxButtons
	if start < 0 {
		start = 0
	}

	for i := len(result) - 1; i >= start; i-- {
		num := len(result) - i
		cmd := result[i]
		display := cmd
		if len(display) > 50 {
			display = display[:47] + "..."
		}
		sb.WriteString(fmt.Sprintf("%d. <code>%s</code>\n", num, escapeHTML(display)))
		rows = append(rows, tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(fmt.Sprintf("▶️ %d: %s", num, truncate(cmd, 30))).
				WithCallbackData(fmt.Sprintf("hist:%d", i)),
		))
	}
	sb.WriteString("\nTap to re-run:")

	return b.sendMarkup(ctx, chatID, sb.String(), tu.InlineKeyboard(rows...))
}

func (b *Bot) handleHistoryCallback(ctx *th.Context, query telego.CallbackQuery) error {
	idxStr := strings.TrimPrefix(query.Data, "hist:")
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return b.send(ctx, chatID, "❌ Invalid history entry.")
	}

	b.historyMu.Lock()
	h := b.history[uid]
	var cmd string
	var found bool
	if idx >= 0 && idx < len(h) {
		cmd = h[idx]
		found = true
	}
	b.historyMu.Unlock()

	if !found {
		return b.send(ctx, chatID, "❌ History entry not found.")
	}

	return b.executeCommand(ctx, chatID, uid, cmd)
}

// --- Persistent Shell ---

func (b *Bot) handleShell(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID

	if !b.ssh.IsConnected(uid) {
		return b.send(ctx, chatID, "⚠️ Not connected. Use /connect first.")
	}

	if b.ssh.HasShell(uid) {
		return b.send(ctx, chatID, "🐚 Shell is already active.\nUse /closeshell to close it.")
	}

	_ = b.send(ctx, chatID, "⏳ Opening persistent shell...")
	if err := b.ssh.OpenShell(uid); err != nil {
		return b.send(ctx, chatID, fmt.Sprintf("❌ Failed to open shell:\n<code>%s</code>", escapeHTML(err.Error())))
	}

	active := b.ssh.ActiveServer(uid)
	return b.send(ctx, chatID,
		fmt.Sprintf("🐚 <b>Persistent shell opened</b> on <b>%s</b>\n\n"+
			"All your commands now run in the same shell session.\n"+
			"State (cd, env vars, etc.) persists between commands.\n\n"+
			"Use /closeshell to close and return to normal mode.",
			escapeHTML(active)))
}

func (b *Bot) handleCloseShell(ctx *th.Context, msg telego.Message) error {
	uid := msg.From.ID
	chatID := msg.Chat.ID

	if !b.ssh.HasShell(uid) {
		return b.send(ctx, chatID, "ℹ️ No active shell session.")
	}

	b.ssh.CloseShell(uid)
	return b.send(ctx, chatID, "🔒 Persistent shell closed. Back to normal mode.")
}

// --- Dangerous Command Confirmation ---

var dangerousPatterns = []string{
	"rm -rf /",
	"rm -fr /",
	"mkfs",
	"dd if=",
	"> /dev/sd",
	"chmod -r 777 /",
	":(){ :|:& };:",
	"reboot",
	"shutdown",
	"halt",
	"poweroff",
	"init 0",
	"init 6",
}

func isDangerousCommand(cmd string) bool {
	lower := strings.ToLower(strings.TrimSpace(cmd))
	for _, p := range dangerousPatterns {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

func (b *Bot) confirmDangerousCommand(ctx context.Context, chatID, userID int64, cmd string, isLive bool) error {
	b.dangerMu.Lock()
	b.dangerCmds[userID] = dangerEntry{cmd: cmd, isLive: isLive}
	b.dangerMu.Unlock()

	markup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("⚠️ Yes, Execute").WithCallbackData(fmt.Sprintf("confirm_danger:%d", userID)),
			tu.InlineKeyboardButton("🚫 Cancel").WithCallbackData("cancel_danger"),
		),
	)

	return b.sendMarkup(ctx, chatID, fmt.Sprintf(
		"⚠️ <b>Dangerous Command Detected</b>\n\n"+
			"<code>%s</code>\n\n"+
			"This command could be destructive. Are you sure?",
		escapeHTML(cmd)), markup)
}

func (b *Bot) handleConfirmDangerousCallback(ctx *th.Context, query telego.CallbackQuery) error {
	_ = b.answer(ctx, query.ID)
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	b.dangerMu.Lock()
	entry, ok := b.dangerCmds[uid]
	if ok {
		delete(b.dangerCmds, uid)
	}
	b.dangerMu.Unlock()

	if !ok {
		return b.send(ctx, chatID, "❌ No pending command found. It may have expired.")
	}

	if entry.isLive {
		return b.startLive(ctx, chatID, uid, entry.cmd)
	}

	cmd := entry.cmd
	// Track in history and execute
	b.addHistory(uid, cmd)
	active := b.ssh.ActiveServer(uid)
	header := fmt.Sprintf("🖥 <b>%s</b> $ <code>%s</code>", escapeHTML(active), escapeHTML(cmd))

	cancelMarkup := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("✋ Cancel").WithCallbackData("cancel_run"),
		),
	)
	ctrlMsg, err := b.bot.SendMessage(ctx,
		tu.Message(tu.ID(chatID), header+"\n<i>⏳ Running...</i>").
			WithParseMode(telego.ModeHTML).
			WithReplyMarkup(cancelMarkup))
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runID := b.draftSeq.Add(1)
	b.runMu.Lock()
	if prev, exists := b.runCxls[uid]; exists {
		prev.cancel()
	}
	b.runCxls[uid] = runEntry{id: runID, cancel: cancel}
	b.runMu.Unlock()

	draftID := int(runID)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("dangerous command panic: %v", r)
			}
			b.runMu.Lock()
			if entry, ok := b.runCxls[uid]; ok && entry.id == runID {
				delete(b.runCxls, uid)
			}
			b.runMu.Unlock()
			cancel()
		}()

		bgCtx := context.Background()
		emptyKb := &telego.InlineKeyboardMarkup{InlineKeyboard: [][]telego.InlineKeyboardButton{}}
		var lastOutput string
		var lastDraft time.Time

		streamErr := b.ssh.ExecStreamRun(runCtx, uid, cmd, b.cfg.MaxOutput, 500*time.Millisecond, 2*time.Minute,
			func(output string, final bool) bool {
				if final {
					lastOutput = output
					return true
				}
				if output != "" && output != lastOutput && time.Since(lastDraft) > 200*time.Millisecond {
					draftText := formatDraftText("🖥 "+active+" $ "+cmd, output)
					_ = b.bot.SendMessageDraft(bgCtx, &telego.SendMessageDraftParams{
						ChatID:  chatID,
						DraftID: draftID,
						Text:    draftText,
					})
					lastOutput = output
					lastDraft = time.Now()
				}
				return true
			})

		_, _ = b.bot.EditMessageText(bgCtx,
			tu.EditMessageText(tu.ID(chatID), ctrlMsg.MessageID, header).
				WithParseMode(telego.ModeHTML).
				WithReplyMarkup(emptyKb))

		output := lastOutput
		if streamErr != nil && output == "" {
			_ = b.send(bgCtx, chatID, "❌ <pre>"+escapeHTML(streamErr.Error())+"</pre>")
			return
		}
		if output == "" {
			output = "(no output)"
		}

		escaped := escapeHTML(output)
		if len(escaped)+20 <= 4096 {
			_ = b.send(bgCtx, chatID, "<pre>"+escaped+"</pre>")
		} else {
			chunks := splitOutput(output, 3900)
			for i, chunk := range chunks {
				esc := escapeHTML(chunk)
				label := ""
				if len(chunks) > 1 {
					label = fmt.Sprintf("📄 Part %d/%d\n", i+1, len(chunks))
				}
				_ = b.send(bgCtx, chatID, label+"<blockquote expandable><pre>"+esc+"</pre></blockquote>")
			}
		}
	}()

	return nil
}

func (b *Bot) handleCancelDangerousCallback(ctx *th.Context, query telego.CallbackQuery) error {
	_ = b.answerText(ctx, query.ID, "🚫 Cancelled.")
	uid := query.From.ID
	chatID := chatIDFromCallback(query)

	b.dangerMu.Lock()
	delete(b.dangerCmds, uid)
	b.dangerMu.Unlock()

	return b.send(ctx, chatID, "🚫 Command cancelled.")
}

// --- utility functions ---

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func splitOutput(s string, maxLen int) []string {
	if len(s) <= maxLen {
		return []string{s}
	}
	var chunks []string
	for len(s) > 0 {
		end := maxLen
		if end > len(s) {
			end = len(s)
		}
		if end < len(s) {
			if idx := strings.LastIndex(s[:end], "\n"); idx > 0 {
				end = idx + 1
			}
		}
		chunks = append(chunks, s[:end])
		s = s[end:]
	}
	return chunks
}

func escapeHTML(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
	)
	return r.Replace(s)
}

func parseArgs(s string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)
	hasQuote := false // tracks if we saw quotes for current token

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote {
			if ch == quoteChar {
				inQuote = false
				continue
			}
			current.WriteByte(ch)
		} else {
			if ch == '"' || ch == '\'' {
				inQuote = true
				quoteChar = ch
				hasQuote = true
				continue
			}
			if ch == ' ' || ch == '\t' {
				if current.Len() > 0 || hasQuote {
					args = append(args, current.String())
					current.Reset()
					hasQuote = false
				}
				continue
			}
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 || hasQuote {
		args = append(args, current.String())
	}
	return args
}
