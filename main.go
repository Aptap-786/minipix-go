// File Name: main.go

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	DataFile  = "user_data.json"
	AnsDbFile = "answers_db.json"
)

type Account struct {
	PhoneNumber  string `json:"phone_number"`
	AccessToken  string `json:"access_token"`
	DeviceId     string `json:"device_id"`
	SessionToken string `json:"session_token,omitempty"`
}

type UserSession struct {
	Accounts      []Account     `json:"accounts"`
	ActiveIndex   int           `json:"active_index"`
	QuizSessionID string        `json:"quiz_session_id,omitempty"`
	CurrentQID    string        `json:"current_question_id,omitempty"`
	CurrentOpts   []interface{} `json:"current_options,omitempty"`
}

var (
	dbMutex        sync.Mutex
	ansMutex       sync.Mutex
	userData       = make(map[int64]*UserSession)
	answersDb      = make(map[string]string)
	wsClients      = make(map[*websocket.Conn]bool)
	wsClientsMutex sync.Mutex
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

func broadcastWs(v interface{}) {
	wsClientsMutex.Lock()
	defer wsClientsMutex.Unlock()
	b, _ := json.Marshal(v)
	for client := range wsClients {
		_ = client.WriteMessage(websocket.TextMessage, b)
	}
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

	fmt.Printf("MiniPIX Backend Server running on port %s\n", port)
	_ = r.Run(":" + port)
}
