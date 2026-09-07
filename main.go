package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	BotToken    = "8903711831:AAEvJXh-sEMBYGx-sSax-wdflzgQIm56vg8"
	DataFile    = "user_data.json"
	AnsDbFile   = "answers_db.json"
	MaxWorkers  = 10
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
}

type Account struct {
	PhoneNumber  string `json:"phone_number"`
	AccessToken  string `json:"access_token"`
	DeviceId     string `json:"device_id"`
	SessionToken string `json:"session_token,omitempty"`
}

type UserSession struct {
	Accounts      []Account       `json:"accounts"`
	ActiveIndex   int             `json:"active_index"`
	TempAcc       *Account        `json:"temp_acc,omitempty"`
	TempRetry     *Account        `json:"temp_retry,omitempty"`
	QuizSessionID string          `json:"quiz_session_id,omitempty"`
	CurrentQID    string          `json:"current_question_id,omitempty"`
	CurrentOpts   []interface{}   `json:"current_options,omitempty"`
}

type TgRequest struct {
	Action    string // "send" or "edit"
	ChatId    int64
	MessageId int
	Text      string
	Markup    map[string]interface{}
	ReplyChan chan int
}

var tgQueue = make(chan TgRequest, 2000)

var (
	dbMutex             sync.Mutex
	ansMutex            sync.Mutex
	userData            = make(map[int64]*UserSession)
	answersDb           = make(map[string]string)
	activeAutoPlay      = make(map[int64]map[int]bool)
	activeAutoPlayMutex sync.Mutex
	wsClients           = make(map[*websocket.Conn]bool)
	wsClientsMutex      sync.Mutex
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Telegram Rate Limiter to prevent flood errors
func initTelegramRateLimiter() {
	ticker := time.NewTicker(350 * time.Millisecond)
	for range ticker.C {
		select {
		case req := <-tgQueue:
			if req.Action == "send" {
				msgId := executeSendMessage(req.ChatId, req.Text, req.Markup)
				if req.ReplyChan != nil {
					req.ReplyChan <- msgId
				}
			} else if req.Action == "edit" {
				executeEditMessage(req.ChatId, req.MessageId, req.Text, req.Markup)
			}
		default:
		}
	}
}

func loadData() {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	if file, err := os.ReadFile(DataFile); err == nil {
		var raw map[string]json.RawMessage
		if json.Unmarshal(file, &raw) == nil {
			for k, v := range raw {
				var chatID int64
				_, _ = fmt.Sscanf(k, "%d", &chatID)
				var session UserSession
				if json.Unmarshal(v, &session) == nil {
					userData[chatID] = &session
				}
			}
		}
	}
	if file, err := os.ReadFile(AnsDbFile); err == nil {
		_ = json.Unmarshal(file, &answersDb)
	}
}

func saveData() {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	if data, err := json.MarshalIndent(userData, "", "    "); err == nil {
		_ = os.WriteFile(DataFile, data, 0644)
	}
}

func saveAnsDb() {
	ansMutex.Lock()
	defer ansMutex.Unlock()
	if data, err := json.MarshalIndent(answersDb, "", "    "); err == nil {
		_ = os.WriteFile(AnsDbFile, data, 0644)
	}
}

func generateDeviceId() string {
	chars := "abcdefghijklmnopqrstuvwxyz0123456789"
	rand.Seed(time.Now().UnixNano())
	var sb strings.Builder
	for i := 0; i < 16; i++ {
		sb.WriteByte(chars[rand.Intn(len(chars))])
	}
	return sb.String()
}

func escapeHtml(text string) string {
	if text == "" {
		return ""
	}
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(text)
}

var baseHeaders = map[string]string{
	"Host":           "api.minipix.co",
	"content-type":   "application/json; charset=utf-8",
	"user-agent":     "okhttp/4.12.0",
	"accept-encoding": "gzip",
}

func askDeepSeek(prompt string) (string, error) {
	url := "https://deep-seek.ai/api/chat"
	fakeIp := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(240)+11, rand.Intn(256), rand.Intn(256), rand.Intn(254)+1)
	currentAgent := userAgents[rand.Intn(len(userAgents))]

	payload := map[string]interface{}{
		"model": "deepseek/deepseek-v4-flash",
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	req.Header.Set("Origin", "https://deep-seek.ai")
	req.Header.Set("Referer", "https://deep-seek.ai/ar/chat")
	req.Header.Set("User-Agent", currentAgent)
	req.Header.Set("X-Forwarded-For", fakeIp)
	req.Header.Set("X-Real-IP", fakeIp)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	var fullText strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data: ") {
			dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if dataStr == "[DONE]" {
				break
			}
			var dataJson map[string]interface{}
			if json.Unmarshal([]byte(dataStr), &dataJson) == nil {
				if choices, ok := dataJson["choices"].([]interface{}); ok && len(choices) > 0 {
					if choice, ok := choices[0].(map[string]interface{}); ok {
						if delta, ok := choice["delta"].(map[string]interface{}); ok {
							if content, ok := delta["content"].(string); ok {
								fullText.WriteString(content)
							}
						}
					}
				}
			}
		}
	}

	result := strings.TrimSpace(fullText.String())
	if result == "" {
		return "", fmt.Errorf("empty response")
	}
	return result, nil
}

func extractAnswer(text string, optionsLen int) int {
	if text == "" {
		return -1
	}
	reNum := regexp.MustCompile(`\b([0-3])\b`)
	match := reNum.FindStringSubmatch(text)
	if len(match) > 1 {
		var idx int
		_, _ = fmt.Sscanf(match[1], "%d", &idx)
		if idx >= 0 && idx < optionsLen {
			return idx
		}
	}
	reAll := regexp.MustCompile(`\d+`)
	nums := reAll.FindAllString(text, -1)
	if len(nums) > 0 {
		var idx int
		_, _ = fmt.Sscanf(nums[0], "%d", &idx)
		if idx >= 0 && idx < optionsLen {
			return idx
		}
	}
	return -1
}

func getAiAnswer(qText string, options []interface{}) int {
	if len(options) == 0 {
		return -1
	}
	var optsStr strings.Builder
	for i, opt := range options {
		optsStr.WriteString(fmt.Sprintf("%d. %v\n", i, opt))
	}

	prompt := fmt.Sprintf(
		"You are an expert English Grammar and Hindi-to-English Translation Teacher.\n"+
			"Select the 100%% correct answer option index (0, 1, 2, or 3) for the given quiz.\n"+
			"RULES: Reply with ONLY a single digit integer (0-3). No text or explanations.\n\n"+
			"Question:\n%s\n\nOptions:\n%s", qText, optsStr.String(),
	)

	for attempt := 0; attempt < 2; attempt++ {
		if ansText, err := askDeepSeek(prompt); err == nil {
			if ans := extractAnswer(ansText, len(options)); ans != -1 {
				return ans
			}
		}
		time.Sleep(1 * time.Second)
	}
	return -1
}

func sendTelegramMessage(chatId int64, text string, replyMarkup interface{}) int {
	replyChan := make(chan int, 1)
	tgQueue <- TgRequest{
		Action:    "send",
		ChatId:    chatId,
		Text:      text,
		Markup:    convertMarkup(replyMarkup),
		ReplyChan: replyChan,
	}
	return <-replyChan
}

func editTelegramMessage(chatId int64, messageId int, text string, replyMarkup interface{}) {
	tgQueue <- TgRequest{
		Action:    "edit",
		ChatId:    chatId,
		MessageId: messageId,
		Text:      text,
		Markup:    convertMarkup(replyMarkup),
	}
}

func convertMarkup(markup interface{}) map[string]interface{} {
	if markup == nil {
		return nil
	}
	if m, ok := markup.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func executeSendMessage(chatId int64, text string, replyMarkup map[string]interface{}) int {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)
	payload := map[string]interface{}{
		"chat_id":    chatId,
		"text":       text,
		"parse_mode": "HTML",
	}
	if replyMarkup != nil {
		payload["reply_markup"] = replyMarkup
	}
	bodyBytes, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	var res map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if result, ok := res["result"].(map[string]interface{}); ok {
		if msgId, ok := result["message_id"].(float64); ok {
			return int(msgId)
		}
	}
	return 0
}

func executeEditMessage(chatId int64, messageId int, text string, replyMarkup map[string]interface{}) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", BotToken)
	payload := map[string]interface{}{
		"chat_id":    chatId,
		"message_id": messageId,
		"text":       text,
		"parse_mode": "HTML",
	}
	if replyMarkup != nil {
		payload["reply_markup"] = replyMarkup
	}
	bodyBytes, _ := json.Marshal(payload)
	_, _ = http.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
}

func getMainChefMenuKeyboard() map[string]interface{} {
	return map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "🟢 Login / Add Account", "callback_data": "menu_login"}},
			{{"text": "✅ Switch/Manage Accounts", "callback_data": "menu_accounts"}},
			{{"text": "🚀 Launch Express Realtime Auto-Play", "callback_data": "mode_launch_all"}},
			{{"text": "🕹️ Play Manually", "callback_data": "mode_manual"}},
			{{"text": "🆘 Support", "url": "https://telegram.me/MoneyScripter"}},
		},
	}
}

func showAccountsMenu(chatId int64, messageId int) {
	dbMutex.Lock()
	session, exists := userData[chatId]
	if !exists || len(session.Accounts) == 0 {
		dbMutex.Unlock()
		markup := map[string]interface{}{
			"inline_keyboard": [][]map[string]string{
				{{"text": "🔙 Menu", "callback_data": "menu_main"}},
			},
		}
		editTelegramMessage(chatId, messageId, "⚠️ No accounts found.", markup)
		return
	}
	accounts := session.Accounts
	activeIdx := session.ActiveIndex
	dbMutex.Unlock()

	var inlineKeyboard [][]map[string]string
	for idx, acc := range accounts {
		status := "⚪"
		if idx == activeIdx {
			status = "✅"
		}
		inlineKeyboard = append(inlineKeyboard, []map[string]string{
			{"text": fmt.Sprintf("%s Acc #%d: +91%s", status, idx+1, acc.PhoneNumber), "callback_data": fmt.Sprintf("switch_%d", idx)},
		})
		inlineKeyboard = append(inlineKeyboard, []map[string]string{
			{"text": fmt.Sprintf("🗑️ Delete Acc #%d", idx+1), "callback_data": fmt.Sprintf("del_%d", idx)},
		})
	}
	inlineKeyboard = append(inlineKeyboard, []map[string]string{
		{"text": "➕ Add Account", "callback_data": "menu_login"},
		{"text": "🏠 Menu", "callback_data": "menu_main"},
	})

	editTelegramMessage(chatId, messageId, "👤 <b>Saved Accounts Management:</b>", map[string]interface{}{
		"inline_keyboard": inlineKeyboard,
	})
}

func broadcastWs(v interface{}) {
	wsClientsMutex.Lock()
	defer wsClientsMutex.Unlock()
	b, _ := json.Marshal(v)
	for client := range wsClients {
		_ = client.WriteMessage(websocket.TextMessage, b)
	}
}

func launchAllAccountsRealtime(chatId int64, messageId int) {
	dbMutex.Lock()
	session, exists := userData[chatId]
	if !exists || len(session.Accounts) == 0 {
		dbMutex.Unlock()
		editTelegramMessage(chatId, messageId, "⚠️ No accounts found to run.", getMainChefMenuKeyboard())
		return
	}
	accounts := session.Accounts
	dbMutex.Unlock()

	editTelegramMessage(chatId, messageId, fmt.Sprintf("🚀 Initializing Express Realtime Engine (Max %d concurrent workers for %d accounts)...", MaxWorkers, len(accounts)), nil)

	activeAutoPlayMutex.Lock()
	if activeAutoPlay[chatId] == nil {
		activeAutoPlay[chatId] = make(map[int]bool)
	}
	activeAutoPlayMutex.Unlock()

	type Item struct {
		Acc Account
		Idx int
	}
	var queue []Item
	for idx, acc := range accounts {
		queue = append(queue, Item{Acc: acc, Idx: idx})
	}

	go processNextBatch(chatId, queue)
}

func processNextBatch(chatId int64, queue []struct {
	Acc Account
	Idx int
}) {
	if len(queue) == 0 {
		return
	}

	var batch []struct {
		Acc Account
		Idx int
	}
	if len(queue) > MaxWorkers {
		batch = queue[:MaxWorkers]
		queue = queue[MaxWorkers:]
	} else {
		batch = queue
		queue = []struct {
			Acc Account
			Idx int
		}{}
	}

	var wg sync.WaitGroup
	for _, item := range batch {
		wg.Add(1)
		activeAutoPlayMutex.Lock()
		activeAutoPlay[chatId][item.Idx] = true
		activeAutoPlayMutex.Unlock()

		go func(acc Account, idx int) {
			defer wg.Done()
			runRealtimeWorker(chatId, idx, acc)
		}(item.Acc, item.Idx)
	}
	wg.Wait()

	if len(queue) > 0 {
		processNextBatch(chatId, queue)
	}
}

func runRealtimeWorker(chatId int64, accIdx int, account Account) {
	authHeaders := make(map[string]string)
	for k, v := range baseHeaders {
		authHeaders[k] = v
	}
	authHeaders["authorization"] = fmt.Sprintf("Bearer %s", account.AccessToken)

	msgId := sendTelegramMessage(chatId, fmt.Sprintf("⚡ [Worker #%d] Connected +91%s", accIdx+1, account.PhoneNumber), map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
		},
	})

	client := &http.Client{Timeout: 15 * time.Second}
	var sessionId string
	var question map[string]interface{}

	req, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/start", bytes.NewBuffer([]byte("")))
	for k, v := range authHeaders {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		editTelegramMessage(chatId, msgId, fmt.Sprintf("⚡ [Worker #%d] Connection Timeout.", accIdx+1), nil)
		return
	}
	defer resp.Body.Close()

	var startRes map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&startRes)
	success, _ := startRes["success"].(bool)
	if !success {
		editTelegramMessage(chatId, msgId, fmt.Sprintf("⚡ [Worker #%d] Start Failed.", accIdx+1), nil)
		return
	}

	if sessObj, ok := startRes["session"].(map[string]interface{}); ok {
		sessionId, _ = sessObj["sessionId"].(string)
	}
	if qObj, ok := startRes["question"].(map[string]interface{}); ok {
		question = qObj
	}

	correct := 0
	wrong := 0

	for {
		activeAutoPlayMutex.Lock()
		if val, exists := activeAutoPlay[chatId]; exists && val[accIdx] == false {
			activeAutoPlayMutex.Unlock()
			editTelegramMessage(chatId, msgId, fmt.Sprintf("🛑 [Worker #%d] Stopped by user.", accIdx+1), nil)
			break
		}
		activeAutoPlayMutex.Unlock()

		if question == nil {
			reqStart, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/start", bytes.NewBuffer([]byte("")))
			for k, v := range authHeaders {
				reqStart.Header.Set(k, v)
			}
			respStart, errs := client.Do(reqStart)
			if errs == nil {
				var sRes map[string]interface{}
				_ = json.NewDecoder(respStart.Body).Decode(&sRes)
				respStart.Body.Close()
				if sOK, _ := sRes["success"].(bool); sOK {
					if sessObj, ok := sRes["session"].(map[string]interface{}); ok {
						sessionId, _ = sessObj["sessionId"].(string)
					}
					if qObj, ok := sRes["question"].(map[string]interface{}); ok {
						question = qObj
						continue
					}
				}
			}
			time.Sleep(60 * time.Second)
			continue
		}

		func() {
			defer func() { recover() }()

			qId, _ := question["questionId"].(string)
			var options []interface{}
			if opts, ok := question["options"].([]interface{}); ok {
				options = opts
			}
			qHi := escapeHtml(fmt.Sprintf("%v", question["questionHi"]))
			qEn := escapeHtml(fmt.Sprintf("%v", question["questionEn"]))
			qIdxFloat, _ := question["index"].(float64)
			qIdx := int(qIdxFloat) + 1
			total := question["total"]

			displayTxt := fmt.Sprintf("⚡ <b>Express Worker #%d (+91%s)</b>\n📝 <b>Q: %d/%v</b>\n%s\n", accIdx+1, account.PhoneNumber, qIdx, total, func() string {
				if qHi != "" && qHi != "<nil>" {
					return qHi
				}
				return qEn
			}())

			broadcastWs(map[string]interface{}{"worker": accIdx + 1, "status": "analyzing"})

			editTelegramMessage(chatId, msgId, displayTxt+"<i>🤖 Express Realtime AI analyzing.</i>", map[string]interface{}{
				"inline_keyboard": [][]map[string]string{
					{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
				},
			})
			time.Sleep(400 * time.Millisecond)

			editTelegramMessage(chatId, msgId, displayTxt+"<i>🤖 Express Realtime AI analyzing..</i>", map[string]interface{}{
				"inline_keyboard": [][]map[string]string{
					{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
				},
			})
			time.Sleep(400 * time.Millisecond)

			qKey := fmt.Sprintf("%s_%s", strings.TrimSpace(fmt.Sprintf("%v", question["questionEn"])), strings.TrimSpace(fmt.Sprintf("%v", question["questionHi"])))
			chosenIndex := -1

			ansMutex.Lock()
			if val, exists := answersDb[qKey]; exists {
				for i, opt := range options {
					if strings.TrimSpace(fmt.Sprintf("%v", opt)) == val {
						chosenIndex = i
						break
					}
				}
			}
			ansMutex.Unlock()

			if chosenIndex == -1 {
				chosenIndex = getAiAnswer(fmt.Sprintf("Hindi: %s\nEnglish: %s", qHi, qEn), options)
				if chosenIndex == -1 {
					time.Sleep(10 * time.Second)
					return
				}
			}

			editTelegramMessage(chatId, msgId, displayTxt+fmt.Sprintf("<i>✅ Express AI Selected Option %d. Submitting...</i>", chosenIndex), map[string]interface{}{
				"inline_keyboard": [][]map[string]string{
					{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
				},
			})
			time.Sleep(500 * time.Millisecond)

			ansPayload, _ := json.Marshal(map[string]interface{}{
				"sessionId":   sessionId,
				"questionId":  qId,
				"chosenIndex": chosenIndex,
			})
			ansReq, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/answer", bytes.NewBuffer(ansPayload))
			for k, v := range authHeaders {
				ansReq.Header.Set(k, v)
			}
			ansResp, err := client.Do(ansReq)
			if err != nil {
				time.Sleep(10 * time.Second)
				return
			}
			defer ansResp.Body.Close()

			var ansData map[string]interface{}
			_ = json.NewDecoder(ansResp.Body).Decode(&ansData)
			success, _ := ansData["success"].(bool)
			if !success {
				time.Sleep(10 * time.Second)
				return
			}

			isCorrect, _ := ansData["correct"].(bool)
			if isCorrect {
				correct++
				editTelegramMessage(chatId, msgId, displayTxt+"<i>✅ Sahi Jawab! (+ Coins)</i>", map[string]interface{}{
					"inline_keyboard": [][]map[string]string{
						{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
					},
				})
			} else {
				wrong++
				if cIdx, ok := ansData["correctIndex"].(float64); ok {
					if int(cIdx) >= 0 && int(cIdx) < len(options) {
						ansMutex.Lock()
						answersDb[qKey] = strings.TrimSpace(fmt.Sprintf("%v", options[int(cIdx)]))
						ansMutex.Unlock()
						saveAnsDb()
					}
				}
				editTelegramMessage(chatId, msgId, displayTxt+"<i>❌ Galat Jawab! Answer saved to Brain.</i>", map[string]interface{}{
					"inline_keyboard": [][]map[string]string{
						{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
					},
				})
			}

			time.Sleep(800 * time.Millisecond)

			nextData, _ := ansData["next"].(map[string]interface{})
			if nextData != nil && nextData["result"] != nil {
				r := nextData["result"].(map[string]interface{})
				editTelegramMessage(chatId, msgId, fmt.Sprintf("🏁 <b>Express Worker #%d Level Completed!</b>\nScore: %v%% | Coins: %v\n✅ %d / ❌ %d", accIdx+1, r["scorePct"], r["coins"], correct, wrong), map[string]interface{}{
					"inline_keyboard": [][]map[string]string{
						{{"text": fmt.Sprintf("🛑 Stop Worker #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
					},
				})
				time.Sleep(10 * time.Second)
				question = nil
			} else if nextData != nil && nextData["question"] != nil {
				question = nextData["question"].(map[string]interface{})
			} else {
				adPayload, _ := json.Marshal(map[string]interface{}{"sessionId": sessionId})
				adReq, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/ad-ack", bytes.NewBuffer(adPayload))
				for k, v := range authHeaders {
					adReq.Header.Set(k, v)
				}
				adResp, errAd := client.Do(adReq)
				if errAd == nil {
					var adData map[string]interface{}
					_ = json.NewDecoder(adResp.Body).Decode(&adData)
					adResp.Body.Close()
					if adOK, _ := adData["success"].(bool); adOK {
						if qObj, ok := adData["question"].(map[string]interface{}); ok {
							question = qObj
							return
						}
					}
				}
				question = nil
			}
		}()
	}
}

func checkAndStartManualQuiz(chatId int64, messageId int) {
	dbMutex.Lock()
	session, exists := userData[chatId]
	if !exists || len(session.Accounts) == 0 {
		dbMutex.Unlock()
		sendTelegramMessage(chatId, "⚠️ Login first.", nil)
		return
	}
	activeIdx := session.ActiveIndex
	if activeIdx >= len(session.Accounts) {
		activeIdx = 0
	}
	acc := session.Accounts[activeIdx]
	dbMutex.Unlock()

	authHeaders := make(map[string]string)
	for k, v := range baseHeaders {
		authHeaders[k] = v
	}
	authHeaders["authorization"] = fmt.Sprintf("Bearer %s", acc.AccessToken)

	client := &http.Client{Timeout: 15 * time.Second}
	req, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/start", bytes.NewBuffer([]byte("")))
	for k, v := range authHeaders {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		sendTelegramMessage(chatId, fmt.Sprintf("⚠️ Error: %s", err.Error()), nil)
		return
	}
	defer resp.Body.Close()

	var res map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&res)
	success, _ := res["success"].(bool)
	if success {
		if sessObj, ok := res["session"].(map[string]interface{}); ok {
			dbMutex.Lock()
			userData[chatId].QuizSessionID, _ = sessObj["sessionId"].(string)
			saveData()
			dbMutex.Unlock()
		}
		if qObj, ok := res["question"].(map[string]interface{}); ok {
			sendQuestionToUser(chatId, qObj)
		}
	} else {
		sendTelegramMessage(chatId, "❌ Failed to start manual quiz.", nil)
	}
}

func sendQuestionToUser(chatId int64, q map[string]interface{}) {
	qId, _ := q["questionId"].(string)
	var options []interface{}
	if opts, ok := q["options"].([]interface{}); ok {
		options = opts
	}

	dbMutex.Lock()
	if userData[chatId] == nil {
		userData[chatId] = &UserSession{}
	}
	userData[chatId].CurrentQID = qId
	userData[chatId].CurrentOpts = options
	saveData()
	dbMutex.Unlock()

	var inlineKeyboard [][]map[string]string
	for idx, opt := range options {
		inlineKeyboard = append(inlineKeyboard, []map[string]string{
			{"text": fmt.Sprintf("%v", opt), "callback_data": fmt.Sprintf("ans_%d", idx)},
		})
	}

	qHi := escapeHtml(fmt.Sprintf("%v", q["questionHi"]))
	qEn := escapeHtml(fmt.Sprintf("%v", q["questionEn"]))
	txtText := qHi
	if txtText == "" || txtText == "<nil>" {
		txtText = qEn
	}

	sendTelegramMessage(chatId, fmt.Sprintf("📝 <b>%s</b>", txtText), map[string]interface{}{
		"inline_keyboard": inlineKeyboard,
	})
}

func processPhoneStep(chatId int64, phoneNumber string) {
	sentMsgId := sendTelegramMessage(chatId, fmt.Sprintf("🔄 Requesting OTP for +91%s...", phoneNumber), nil)

	payload, _ := json.Marshal(map[string]string{"phone_number": phoneNumber})
	resp, err := http.Post("https://api.minipix.co/v4/login/generate-otp", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		editTelegramMessage(chatId, sentMsgId, fmt.Sprintf("⚠️ Error: %s", err.Error()), nil)
		return
	}
	defer resp.Body.Close()

	var resData map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&resData)
	sessionToken, _ := resData["session_token"].(string)
	if sessionToken == "" {
		editTelegramMessage(chatId, sentMsgId, "❌ OTP Request Failed.", nil)
		return
	}

	dbMutex.Lock()
	if userData[chatId] == nil {
		userData[chatId] = &UserSession{}
	}
	userData[chatId].TempAcc = &Account{
		PhoneNumber:  phoneNumber,
		SessionToken: sessionToken,
		DeviceId:     generateDeviceId(),
	}
	saveData()
	dbMutex.Unlock()

	editTelegramMessage(chatId, sentMsgId, "✉️ Enter received OTP code:", nil)
}

func processOtpStep(chatId int64, otpCode string) {
	dbMutex.Lock()
	session := userData[chatId]
	if session == nil || session.TempAcc == nil {
		dbMutex.Unlock()
		return
	}
	tempAcc := *session.TempAcc
	dbMutex.Unlock()

	verifyPayload, _ := json.Marshal(map[string]string{
		"client_id":     "android",
		"device_id":     tempAcc.DeviceId,
		"device_info":   "vivo",
		"otp":           otpCode,
		"phone_number":  tempAcc.PhoneNumber,
		"session_token": tempAcc.SessionToken,
	})

	resp, err := http.Post("https://api.minipix.co/v4/login/verify-otp", "application/json", bytes.NewBuffer(verifyPayload))
	if err != nil {
		sendTelegramMessage(chatId, fmt.Sprintf("⚠️ Verify Error: %s", err.Error()), nil)
		return
	}
	defer resp.Body.Close()

	var resData map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&resData)
	accessToken, _ := resData["access_token"].(string)

	if accessToken == "" {
		dbMutex.Lock()
		userData[chatId].TempRetry = &tempAcc
		userData[chatId].TempAcc = nil
		saveData()
		dbMutex.Unlock()

		sendTelegramMessage(chatId, "⚠️ Device limit hit. Use Access Token instead:", map[string]interface{}{
			"inline_keyboard": [][]map[string]string{
				{{"text": "🔑 Login via Token", "callback_data": "enter_access_token"}},
			},
		})
		return
	}

	dbMutex.Lock()
	if userData[chatId].Accounts == nil {
		userData[chatId].Accounts = []Account{}
	}
	userData[chatId].Accounts = append(userData[chatId].Accounts, Account{
		PhoneNumber: tempAcc.PhoneNumber,
		AccessToken: accessToken,
		DeviceId:    tempAcc.DeviceId,
	})
	userData[chatId].ActiveIndex = len(userData[chatId].Accounts) - 1
	userData[chatId].TempAcc = nil
	saveData()
	dbMutex.Unlock()

	sendTelegramMessage(chatId, "✅ Account saved successfully!", nil)
	sendTelegramMessage(chatId, "Main Menu:", getMainChefMenuKeyboard())
}

func processAccessTokenStep(chatId int64, token string) {
	dbMutex.Lock()
	session := userData[chatId]
	if session == nil || session.TempRetry == nil {
		dbMutex.Unlock()
		return
	}
	temp := *session.TempRetry
	if userData[chatId].Accounts == nil {
		userData[chatId].Accounts = []Account{}
	}
	userData[chatId].Accounts = append(userData[chatId].Accounts, Account{
		PhoneNumber: temp.PhoneNumber,
		AccessToken: token,
		DeviceId:    temp.DeviceId,
	})
	userData[chatId].ActiveIndex = len(userData[chatId].Accounts) - 1
	userData[chatId].TempRetry = nil
	saveData()
	dbMutex.Unlock()

	sendTelegramMessage(chatId, "✅ Token saved!", nil)
	sendTelegramMessage(chatId, "Menu:", getMainChefMenuKeyboard())
}

func startTelegramPolling() {
	offset := 0
	client := &http.Client{Timeout: 30 * time.Second}

	// We can track pending states for text inputs per chat
	pendingState := make(map[string]string) // "chatId_phone" or "chatId_otp" or "chatId_token"

	for {
		url := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?offset=%d&timeout=25", BotToken, offset)
		resp, err := client.Get(url)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}

		var result struct {
			Ok     bool `json:"ok"`
			Result []struct {
				UpdateId int `json:"update_id"`
				Message  *struct {
					Chat struct {
						Id int64 `json:"id"`
					} `json:"chat"`
					Text string `json:"text"`
				} `json:"message"`
				CallbackQuery *struct {
					Id   string `json:"id"`
					Data string `json:"data"`
					Message *struct {
						Chat struct {
							Id int64 `json:"id"`
						} `json:"chat"`
						MessageId int `json:"message_id"`
					} `json:"message"`
				} `json:"callback_query"`
			} `json:"result"`
		}

		if json.NewDecoder(resp.Body).Decode(&result) == nil && result.Ok {
			for _, update := range result.Result {
				offset = update.UpdateId + 1

				if update.Message != nil {
					chatId := update.Message.Chat.Id
					text := strings.TrimSpace(update.Message.Text)

					if text == "/start" {
						dbMutex.Lock()
						if userData[chatId] == nil {
							userData[chatId] = &UserSession{Accounts: []Account{}, ActiveIndex: 0}
							saveData()
						}
						dbMutex.Unlock()
						sendTelegramMessage(chatId, "👋 <b>MiniPIX Express + WebSocket Realtime Bot Online!</b>\nSelect option below:", getMainChefMenuKeyboard())
						delete(pendingState, fmt.Sprintf("%d", chatId))
					} else {
						stateKey := fmt.Sprintf("%d", chatId)
						state := pendingState[stateKey]
						if state == "waiting_phone" {
							delete(pendingState, stateKey)
							go processPhoneStep(chatId, text)
						} else if state == "waiting_otp" {
							delete(pendingState, stateKey)
							go processOtpStep(chatId, text)
						} else if state == "waiting_token" {
							delete(pendingState, stateKey)
							go processAccessTokenStep(chatId, text)
						}
					}
				} else if update.CallbackQuery != nil {
					call := update.CallbackQuery
					chatId := call.Message.Chat.Id
					messageId := call.Message.MessageId
					data := call.Data

					if data == "menu_main" {
						editTelegramMessage(chatId, messageId, "👋 <b>Main Menu:</b>", getMainChefMenuKeyboard())
					} else if data == "menu_login" {
						pendingState[fmt.Sprintf("%d", chatId)] = "waiting_phone"
						sendTelegramMessage(chatId, "📱 Enter your 10-digit mobile number:", nil)
					} else if data == "menu_accounts" {
						showAccountsMenu(chatId, messageId)
					} else if data == "mode_launch_all" {
						launchAllAccountsRealtime(chatId, messageId)
					} else if strings.HasPrefix(data, "stop_acc_") {
						var accIdx int
						_, _ = fmt.Sscanf(data, "stop_acc_%d", &accIdx)
						activeAutoPlayMutex.Lock()
						if activeAutoPlay[chatId] != nil {
							activeAutoPlay[chatId][accIdx] = false
						}
						activeAutoPlayMutex.Unlock()
					} else if data == "mode_manual" {
						checkAndStartManualQuiz(chatId, messageId)
					} else if strings.HasPrefix(data, "switch_") {
						var accIdx int
						_, _ = fmt.Sscanf(data, "switch_%d", &accIdx)
						dbMutex.Lock()
						if userData[chatId] != nil && accIdx < len(userData[chatId].Accounts) {
							userData[chatId].ActiveIndex = accIdx
							saveData()
							showAccountsMenu(chatId, messageId)
						}
						dbMutex.Unlock()
					} else if strings.HasPrefix(data, "del_") {
						var accIdx int
						_, _ = fmt.Sscanf(data, "del_%d", &accIdx)
						dbMutex.Lock()
						if userData[chatId] != nil && userData[chatId].Accounts != nil {
							userData[chatId].Accounts = append(userData[chatId].Accounts[:accIdx], userData[chatId].Accounts[accIdx+1:]...)
							saveData()
							showAccountsMenu(chatId, messageId)
						}
						dbMutex.Unlock()
					} else if data == "enter_access_token" {
						dbMutex.Lock()
						if userData[chatId] != nil && userData[chatId].TempRetry != nil {
							pendingState[fmt.Sprintf("%d", chatId)] = "waiting_token"
							sendTelegramMessage(chatId, "🔑 Send Access Token (eyJ...):", nil)
						}
						dbMutex.Unlock()
					} else if strings.HasPrefix(data, "ans_") {
						var chosenIndex int
						_, _ = fmt.Sscanf(data, "ans_%d", &chosenIndex)

						dbMutex.Lock()
						session := userData[chatId]
						if session != nil && session.QuizSessionID != "" {
							acc := session.Accounts[session.ActiveIndex]
							sessionId := session.QuizSessionID
							qId := session.CurrentQID
							dbMutex.Unlock()

							authHeaders := make(map[string]string)
							for k, v := range baseHeaders {
								authHeaders[k] = v
							}
							authHeaders["authorization"] = fmt.Sprintf("Bearer %s", acc.AccessToken)

							ansPayload, _ := json.Marshal(map[string]interface{}{
								"sessionId":   sessionId,
								"questionId":  qId,
								"chosenIndex": chosenIndex,
							})
							ansResp, err := http.Post("https://api.minipix.co/v4/quiz/session/answer", "application/json", bytes.NewBuffer(ansPayload))
							if err == nil {
								var dataMap map[string]interface{}
								_ = json.NewDecoder(ansResp.Body).Decode(&dataMap)
								ansResp.Body.Close()

								if isCorr, _ := dataMap["correct"].(bool); isCorr {
									sendTelegramMessage(chatId, fmt.Sprintf("✅ Sahi Jawab! Coins: %v", dataMap["coinsSoFar"]), nil)
								} else {
									sendTelegramMessage(chatId, "❌ Galat Jawab!", nil)
								}

								if next, ok := dataMap["next"].(map[string]interface{}); ok {
									if qN, ok := next["question"].(map[string]interface{}); ok {
										sendQuestionToUser(chatId, qN)
									}
								}
							}
						} else {
							dbMutex.Unlock()
						}
					}
				}
			}
		}
		resp.Body.Close()
	}
}

func main() {
	loadData()

	go initTelegramRateLimiter()
	go startTelegramPolling()

	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte("<h1>MiniPIX Express Realtime Worker Server is Running Smoothly! 🚀</h1>"))
	})

	r.GET("/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		wsClientsMutex.Lock()
		wsClients[conn] = true
		wsClientsMutex.Unlock()

		defer func() {
			wsClientsMutex.Lock()
			delete(wsClients, conn)
			wsClientsMutex.Unlock()
			conn.Close()
		}()

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	fmt.Printf("Express Server and WebSocket Engine running on port %s\n", port)
	_ = r.Run(":" + port)
}
