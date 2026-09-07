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
	BotToken   = "8600466949:AAHlDM_-5wF1wiOWCd-NvryKZS8gIc1cK7w"
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

func main() {
	loadData()

	go initTelegramRateLimiter()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<h1>Go High-Performance Quiz Worker Server Online (Rate-Limit Proof) 🚀</h1>"))
	})

	go func() {
		_ = http.ListenAndServe(Port, nil)
	}()

	fmt.Printf("Go Server running on port %s with Rate Limiter active...\n", Port)
	select {}
}
