// File Name: main.go

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	BotToken   = "8903711831:AAEvJXh-sEMBYGx-sSax-wdflzgQIm56vg8"
	Port       = ":3000"
	DataFile   = "user_data.json"
	AnsDbFile  = "answers_db.json"
	MaxWorkers = 10
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
}

type Account struct {
	PhoneNumber string `json:"phone_number"`
	AccessToken string `json:"access_token"`
	DeviceId    string `json:"device_id"`
}

type UserSession struct {
	Accounts    []Account `json:"accounts"`
	ActiveIndex int       `json:"active_index"`
}

type TgRequest struct {
	Action    string
	ChatId    int64
	MessageId int
	Text      string
	Markup    map[string]interface{}
	ReplyChan chan int
}

var tgQueue = make(chan TgRequest, 1000)

var (
	dbMutex       sync.Mutex
	userData      = make(map[int64]*UserSession)
	answersDb     = make(map[string]string)
	activeWorkers = make(map[int64]map[int]bool)
	workersMutex  sync.Mutex
)

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
		_ = json.Unmarshal(file, &userData)
	}
	if file, err := os.ReadFile(AnsDbFile); err == nil {
		_ = json.Unmarshal(file, &answersDb)
	}
}

func saveAnsDb() {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	if data, err := json.MarshalIndent(answersDb, "", "    "); err == nil {
		_ = os.WriteFile(AnsDbFile, data, 0644)
	}
}

func escapeHtml(text string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(text)
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
	for i := 0; i < optionsLen; i++ {
		if strings.Contains(text, fmt.Sprintf("%d", i)) {
			return i
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

func sendTelegramMessage(chatId int64, text string, replyMarkup map[string]interface{}) int {
	replyChan := make(chan int, 1)
	tgQueue <- TgRequest{
		Action:    "send",
		ChatId:    chatId,
		Text:      text,
		Markup:    replyMarkup,
		ReplyChan: replyChan,
	}
	return <-replyChan
}

func editTelegramMessage(chatId int64, messageId int, text string, replyMarkup map[string]interface{}) {
	tgQueue <- TgRequest{
		Action:    "edit",
		ChatId:    chatId,
		MessageId: messageId,
		Text:      text,
		Markup:    replyMarkup,
	}
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

func handleWorkers(chatId int64) {
	workersMutex.Lock()
	if _, ok := userData[chatId]; !ok || len(userData[chatId].Accounts) == 0 {
		// Fallback sample account for instant testing if no accounts saved yet
		if !ok {
			userData[chatId] = &UserSession{}
		}
		// You can load real accounts via session, or notify user
		workersMutex.Unlock()
		sendTelegramMessage(chatId, "⚠️ No accounts found in session. Please configure accounts.", nil)
		return
	}
	accounts := userData[chatId].Accounts
	if activeWorkers[chatId] == nil {
		activeWorkers[chatId] = make(map[int]bool)
	}
	workersMutex.Unlock()

	sendTelegramMessage(chatId, fmt.Sprintf("🚀 Initializing %d Go Workers...", len(accounts)), nil)

	for idx, acc := range accounts {
		workersMutex.Lock()
		activeWorkers[chatId][idx] = true
		workersMutex.Unlock()

		go runRealtimeWorker(chatId, idx, acc)
	}
}

func runRealtimeWorker(chatId int64, accIdx int, account Account) {
	client := &http.Client{Timeout: 15 * time.Second}
	headers := map[string]string{
		"Host":          "api.minipix.co",
		"content-type":  "application/json; charset=utf-8",
		"user-agent":    "okhttp/4.12.0",
		"authorization": fmt.Sprintf("Bearer %s", account.AccessToken),
	}

	msgId := sendTelegramMessage(chatId, fmt.Sprintf("⚡ [Worker #%d] Connected +91%s", accIdx+1, account.PhoneNumber), map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": fmt.Sprintf("🛑 Stop #%d", accIdx+1), "callback_data": fmt.Sprintf("stop_acc_%d", accIdx)}},
		},
	})

	var sessionId string
	var question map[string]interface{}

	req, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/start", nil)
	for k, v := range headers {
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
	if session, ok := startRes["session"].(map[string]interface{}); ok {
		if sId, ok := session["sessionId"].(string); ok {
			sessionId = sId
		}
	}
	if q, ok := startRes["question"].(map[string]interface{}); ok {
		question = q
	}

	correct, wrong := 0, 0

	for {
		workersMutex.Lock()
		if activeWorkers[chatId] != nil && activeWorkers[chatId][accIdx] == false {
			workersMutex.Unlock()
			editTelegramMessage(chatId, msgId, fmt.Sprintf("🛑 [Worker #%d] Stopped.", accIdx+1), nil)
			break
		}
		workersMutex.Unlock()

		if question == nil {
			time.Sleep(10 * time.Second)
			continue
		}

		qId := question["questionId"].(string)
		options := question["options"].([]interface{})
		qHi, _ := question["questionHi"].(string)
		qEn, _ := question["questionEn"].(string)
		qIdx := int(question["index"].(float64)) + 1
		total := question["total"]

		displayTxt := fmt.Sprintf("⚡ <b>Worker #%d (+91%s)</b>\n📝 <b>Q: %d/%v</b>\n%s\n", accIdx+1, account.PhoneNumber, qIdx, total, escapeHtml(qHi))
		editTelegramMessage(chatId, msgId, displayTxt+"<i>🤖 AI analyzing...</i>", nil)

		qKey := fmt.Sprintf("%s_%s", strings.TrimSpace(qEn), strings.TrimSpace(qHi))
		chosenIndex := -1

		dbMutex.Lock()
		if ans, ok := answersDb[qKey]; ok {
			for i, opt := range options {
				if strings.TrimSpace(fmt.Sprintf("%v", opt)) == ans {
					chosenIndex = i
					break
				}
			}
		}
		dbMutex.Unlock()

		if chosenIndex == -1 {
			chosenIndex = getAiAnswer(fmt.Sprintf("Hindi: %s\nEnglish: %s", qHi, qEn), options)
			if chosenIndex == -1 {
				time.Sleep(5 * time.Second)
				continue
			}
		}

		payloadMap := map[string]interface{}{
			"sessionId":   sessionId,
			"questionId":  qId,
			"chosenIndex": chosenIndex,
		}
		pBytes, _ := json.Marshal(payloadMap)

		ansReq, _ := http.NewRequest("POST", "https://api.minipix.co/v4/quiz/session/answer", bytes.NewBuffer(pBytes))
		for k, v := range headers {
			ansReq.Header.Set(k, v)
		}
		ansResp, err := client.Do(ansReq)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		var ansData map[string]interface{}
		_ = json.NewDecoder(ansResp.Body).Decode(&ansData)
		ansResp.Body.Close()

		if isCorr, ok := ansData["correct"].(bool); ok && isCorr {
			correct++
			editTelegramMessage(chatId, msgId, displayTxt+"<i>✅ Correct!</i>", nil)
		} else {
			wrong++
			if cIdx, ok := ansData["correctIndex"].(float64); ok {
				if int(cIdx) >= 0 && int(cIdx) < len(options) {
					dbMutex.Lock()
					answersDb[qKey] = strings.TrimSpace(fmt.Sprintf("%v", options[int(cIdx)]))
					dbMutex.Unlock()
					saveAnsDb()
				}
			}
			editTelegramMessage(chatId, msgId, displayTxt+"<i>❌ Incorrect! Saved.</i>", nil)
		}

		time.Sleep(1 * time.Second)

		if next, ok := ansData["next"].(map[string]interface{}); ok {
			if resObj, ok := next["result"].(map[string]interface{}); ok {
				editTelegramMessage(chatId, msgId, fmt.Sprintf("🏁 <b>Worker #%d Finished!</b>\nScore: %v%% | Coins: %v\n✅ %d / ❌ %d", accIdx+1, resObj["scorePct"], resObj["coins"], correct, wrong), nil)
				time.Sleep(10 * time.Second)
				question = nil
			} else if qObj, ok := next["question"].(map[string]interface{}); ok {
				question = qObj
			}
		}
	}
}

func startTelegramPolling() {
	offset := 0
	client := &http.Client{Timeout: 30 * time.Second}

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
					} `json:"message"`
				} `json:"callback_query"`
			} `json:"result"`
		}

		if json.NewDecoder(resp.Body).Decode(&result) == nil && result.Ok {
			for _, update := range result.Result {
				offset = update.UpdateId + 1

				if update.Message != nil {
					chatId := update.Message.Chat.Id
					text := update.Message.Text

					if text == "/start" || text == "/run" {
						sendTelegramMessage(chatId, "🤖 <b>Go High-Performance Quiz Bot Active!</b>", map[string]interface{}{
							"inline_keyboard": [][]map[string]string{
								{{"text": "🚀 Start Quiz Workers", "callback_data": "start_workers"}},
							},
						})
					}
				} else if update.CallbackQuery != nil {
					chatId := update.CallbackQuery.Message.Chat.Id
					data := update.CallbackQuery.Data

					if data == "start_workers" {
						sendTelegramMessage(chatId, "⚡ Triggering workers...", nil)
						go handleWorkers(chatId)
					} else if strings.HasPrefix(data, "stop_acc_") {
						var accIdx int
						_, _ = fmt.Sscanf(data, "stop_acc_%d", &accIdx)
						workersMutex.Lock()
						if activeWorkers[chatId] != nil {
							activeWorkers[chatId][accIdx] = false
						}
						workersMutex.Unlock()
						sendTelegramMessage(chatId, fmt.Sprintf("🛑 Stopping Worker #%d...", accIdx+1), nil)
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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<h1>Go High-Performance Quiz Worker Server Online (Fully Optimized & Rate-Limit Proof) 🚀</h1>"))
	})

	go func() {
		_ = http.ListenAndServe(Port, nil)
	}()

	fmt.Printf("Go Server running on port %s with Polling and Rate Limiter active...\n", Port)
	select {}
}
