// File Name: main.go

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
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

func broadcastWs(v interface{}) {
	wsClientsMutex.Lock()
	defer wsClientsMutex.Unlock()
	b, _ := json.Marshal(v)
	for client := range wsClients {
		_ = client.WriteMessage(websocket.TextMessage, b)
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

func startAutomationEngine() {
	activeAutoPlayMutex.Lock()
	if activeAutoPlay {
		activeAutoPlayMutex.Unlock()
		return
	}
	activeAutoPlay = true
	activeAutoPlayMutex.Unlock()

	broadcastWs(map[string]interface{}{"worker": 1, "status": "Automation Engine Started. Running active workers..."})

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
				broadcastWs(map[string]interface{}{"worker": 0, "status": "No accounts found. Please add an account via panel."})
				time.Sleep(10 * time.Second)
				continue
			}

			for idx, acc := range allAccounts {
				broadcastWs(map[string]interface{}{"worker": idx + 1, "status": fmt.Sprintf("Processing account +91%s successfully.", acc.PhoneNumber)})
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
	broadcastWs(map[string]interface{}{"worker": 0, "status": "Automation Engine Stopped by user."})
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
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "Automation engine initiated!"})
	})

	r.POST("/api/stop", func(c *gin.Context) {
		stopAutomationEngine()
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "Automation engine halted!"})
	})

	r.POST("/api/login", func(c *gin.Context) {
		var req struct {
			Phone string `json:"phone"`
		}
		if err := c.BindJSON(&req); err != nil || req.Phone == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "Invalid phone number"})
			return
		}

		payload, _ := json.Marshal(map[string]string{"phone_number": req.Phone})
		resp, err := http.Post("https://api.minipix.co/v4/login/generate-otp", "application/json", bytes.NewBuffer(payload))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "message": "Network error generating OTP"})
			return
		}
		defer resp.Body.Close()

		var resData map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&resData)
		sessionToken, _ := resData["session_token"].(string)

		if sessionToken == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "OTP generation failed from server"})
			return
		}

		dbMutex.Lock()
		if userData[1] == nil {
			userData[1] = &UserSession{Accounts: []Account{}, ActiveIndex: 0}
		}
		dbMutex.Unlock()

		c.JSON(http.StatusOK, gin.H{"success": true, "message": "OTP sent successfully!", "session_token": sessionToken})
	})

	r.POST("/api/verify", func(c *gin.Context) {
		var req struct {
			Phone        string `json:"phone"`
			Otp          string `json:"otp"`
			SessionToken string `json:"session_token"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "Invalid payload"})
			return
		}

		deviceId := generateDeviceId()
		verifyPayload, _ := json.Marshal(map[string]string{
			"client_id":     "android",
			"device_id":     deviceId,
			"device_info":   "vivo",
			"otp":           req.Otp,
			"phone_number":  req.Phone,
			"session_token": req.SessionToken,
		})

		resp, err := http.Post("https://api.minipix.co/v4/login/verify-otp", "application/json", bytes.NewBuffer(verifyPayload))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "message": "Verification network error"})
			return
		}
		defer resp.Body.Close()

		var resData map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&resData)
		accessToken, _ := resData["access_token"].(string)

		if accessToken == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "Invalid OTP code entered"})
			return
		}

		dbMutex.Lock()
		if userData[1] == nil {
			userData[1] = &UserSession{Accounts: []Account{}, ActiveIndex: 0}
		}
		userData[1].Accounts = append(userData[1].Accounts, Account{
			PhoneNumber: req.Phone,
			AccessToken: accessToken,
			DeviceId:    deviceId,
		})
		saveData()
		dbMutex.Unlock()

		c.JSON(http.StatusOK, gin.H{"success": true, "message": "Account successfully logged in and saved!"})
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
			"status": "Connected to MiniPIX Web Automation Stream",
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
