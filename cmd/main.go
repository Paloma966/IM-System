package main

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

type Message struct {
	From string `json:"from"`
	To   string `json:"to"`
	Text string `json:"text"`
}

type SSEEvent struct {
	From string `json:"from"`
	To   string `json:"to"`
	Text string `json:"text"`
}

type ConnectRequest struct {
	Name string `json:"name"`
}

var users = make(map[string]bool)
var inboxs = make(map[string][]string)
var mu sync.RWMutex
var subscribers = make(map[string]chan string)

func main() {
	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == http.MethodOptions {
			c.Status(http.StatusNoContent)
			return
		}

		c.Next()
	})

	r.Static("/assets", "./web/dist/assets")
	r.StaticFile("/favicon.ico", "./web/dist/favicon.ico")

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	r.GET("/", func(c *gin.Context) {
		c.File("./web/dist/index.html")
	})

	r.GET("/user/:name", func(c *gin.Context) {
		name := c.Param("name")
		c.JSON(http.StatusOK, gin.H{"user": name})
	})

	r.GET("/search", func(c *gin.Context) {
		q := c.Query("q")
		c.JSON(http.StatusOK, gin.H{"query": q})
	})

	r.POST("/api/messages", func(c *gin.Context) {
		var msg Message
		if err := c.ShouldBindJSON(&msg); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		mu.RLock()
		defer mu.RUnlock()

		event := SSEEvent{From: msg.From, To: msg.To, Text: msg.Text}
		data, _ := json.Marshal(event)

		if msg.To == "" || msg.To == "all" {
			for name, ch := range subscribers {
				select {
				case ch <- string(data):
				default:
				}
				_ = name
			}
			c.JSON(http.StatusOK, gin.H{"ok": true})
			return
		}

		if ch, ok := subscribers[msg.To]; ok {
			select {
			case ch <- string(data):
			default:
			}
		}

		if msg.From != msg.To {
			if ch, ok := subscribers[msg.From]; ok {
				select {
				case ch <- string(data):
				default:
				}
			}
		}

		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.POST("/connect", func(c *gin.Context) {
		var req ConnectRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		mu.Lock()
		defer mu.Unlock()
		users[req.Name] = true
		subscribers[req.Name] = make(chan string, 10)

		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.GET("/users", func(c *gin.Context) {
		mu.RLock()
		defer mu.RUnlock()

		list := make([]string, 0, len(users))
		for name := range users {
			list = append(list, name)
		}
		c.JSON(http.StatusOK, gin.H{"users": list})
	})

	r.GET("/inbox/:name", func(c *gin.Context) {
		name := c.Param("name")

		mu.Lock()
		defer mu.Unlock()
		msgs := inboxs[name]
		inboxs[name] = nil

		if msgs == nil {
			msgs = []string{}
		}
		c.JSON(http.StatusOK, gin.H{"message": msgs})
	})

	r.GET("/stream/:name", func(c *gin.Context) {
		name := c.Param("name")

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")

		mu.RLock()
		ch, ok := subscribers[name]
		mu.RUnlock()

		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "user not connected"})
			return
		}

		for {
			select {
			case msg := <-ch:
				c.SSEvent("message", msg)
				c.Writer.Flush()
			case <-c.Request.Context().Done():
				mu.Lock()
				delete(subscribers, name)
				delete(users, name)
				mu.Unlock()
				return
			}
		}
	})

	r.Run()
}
