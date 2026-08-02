package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Server struct {
	IP       string
	Port     int
	Users    map[string]*User
	mu       sync.RWMutex
	Messages chan string
}

func NewServer(ip string, port int) *Server {
	server := &Server{
		IP:       ip,
		Port:     port,
		Users:    make(map[string]*User),
		Messages: make(chan string),
	}
	return server
}

func (s *Server) HandleMessages() {
	for {
		msg := <-s.Messages

		s.mu.Lock()
		for _, cli := range s.Users {
			cli.MsgCh <- msg
		}
		s.mu.Unlock()
	}
}

func (s *Server) Broadcast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + ": " + msg
	s.Messages <- sendMsg
}

func (s *Server) Handler(conn net.Conn) {
	user := NewUser(conn, s)

	user.Online()

	go user.ListenMessage()

	isLive := make(chan bool)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n == 0 {
				user.Offline()
				return
			}
			if err != nil && err != io.EOF {
				fmt.Println("conn Read err:", err)
				return
			}

			msg := string(buf[:n-1])
			user.HandleMessage(msg)

			isLive <- true
		}
	}()

	for {
		select {
		case <-isLive:
		case <-time.After(time.Second * 60):
			user.SendMsg("You have been kicked out")
			close(user.MsgCh)
			conn.Close()
			return
		}
	}
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.IP, s.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	defer listener.Close()

	go s.HandleMessages()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept err:", err)
			continue
		}

		go s.Handler(conn)
	}
}
