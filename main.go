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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	DataFile   = "user_data.json"
	AnsDbFile  = "answers_db.json"
	MaxWorkers = 10
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
}

type Account struct {
	PhoneNumber  string `json:"phone_number"`
	AccessToken  string `json:"access_token"`
	DeviceId     string `json:"device_id"`
	SessionToken string `json:"session_token,omitempty"`
}

type UserSession struct {
	Accounts    []Account `json:"accounts"`
	ActiveIndex int       `json:"active_index"`
}

var (
	dbMutex             sync.Mutex
	ansMutex            sync.Mutex
	userData            = make(map[int64]*UserSession)
	answersDb           = make(map[string]string)
	wsClients           = make(map[*websocket.Conn]bool)
	wsClientsMutex      sync.Mutex
	activeAutoPlay      = false
	activeAutoPlayMutex sync.Mutex
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
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

func broadcastWs(v interface{}) {
	wsClientsMutex.Lock()
	defer wsClientsMutex.Unlock()
	b, _ := json.Marshal(v)
	for client := range wsClients {
		_ = client.WriteMessage(websocket.TextMessage, b)
	}
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

func startAutomationEngine() {
	activeAutoPlayMutex.Lock()
	if activeAutoPlay {
		activeAutoPlayMutex.Unlock()
		return
	}
	activeAutoPlay = true
	activeAutoPlayMutex.Unlock()

	broadcastWs(map[string]interface{}{"worker": 1, "status": "Automation Engine Started. Analyzing active accounts..."})

	go func() {
		for {
			activeAutoPlayMutex.Lock()
			running := activeAutoPlay
			activeAutoPlayMutex.Unlock()

			if !running {
				break
			}

			dbMutex.Lock()
			var allAccounts []Account
			for _, sess := range userData {
				allAccounts = append(allAccounts, sess.Accounts...)
			}
			dbMutex.Unlock()

			if len(allAccounts) == 0 {
				broadcastWs(map[string]interface{}{"worker": 0, "status": "No accounts found in database. Please add accounts."})
				time.Sleep(10 * time.Second)
				continue
			}

			for idx, acc := range allAccounts {
				broadcastWs(map[string]interface{}{"worker": idx + 1, "status": fmt.Sprintf("Processing account +91%s", acc.PhoneNumber)})
				time.Sleep(3 * time.Second)
			}

			time.Sleep(15 * time.Second)
		}
	}()
}

func stopAutomationEngine() {
	activeAutoPlayMutex.Lock()
	activeAutoPlay = false
	activeAutoPlayMutex.Unlock()
	broadcastWs(map[string]interface{}{"worker": 0, "status": "Automation Engine Stopped."})
}

func main() {
	loadData()

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	r.GET("/", func(c *gin.Context) {
		c.File("index.html")
	})

	r.POST("/api/start", func(c *gin.Context) {
		go startAutomationEngine()
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "Automation started successfully!"})
	})

	r.POST("/api/stop", func(c *gin.Context) {
		stopAutomationEngine()
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "Automation stopped successfully!"})
	})

	r.GET("/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		wsClientsMutex.Lock()
		wsClients[conn] = true
		wsClientsMutex.Unlock()

		_ = conn.WriteJSON(map[string]interface{}{
			"worker": 0,
			"status": "Connected to MiniPIX Automation Server",
		})

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

	fmt.Printf("MiniPIX Backend Server running on port %s\n", port)
	_ = r.Run(":" + port)
}
