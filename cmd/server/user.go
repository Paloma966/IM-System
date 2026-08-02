package main

import (
	"net"
	"strings"
)

type User struct {
	Name   string
	Addr   string
	MsgCh  chan string
	conn   net.Conn
	server *Server
}

func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()

	user := &User{
		Name:   userAddr,
		Addr:   userAddr,
		MsgCh:  make(chan string),
		conn:   conn,
		server: server,
	}
	return user
}

func (u *User) Online() {
	u.server.mu.Lock()
	u.server.Users[u.Name] = u
	u.server.mu.Unlock()

	u.server.Broadcast(u, "Online...")
}

func (u *User) Offline() {
	u.server.mu.Lock()
	delete(u.server.Users, u.Name)
	u.server.mu.Unlock()

	u.server.Broadcast(u, "Offline...")
}

func (u *User) SendMsg(msg string) {
	u.conn.Write([]byte(msg))
}

func (u *User) HandleMessage(msg string) {
	if msg == "who" {
		u.server.mu.Lock()
		for _, user := range u.server.Users {
			onlineMsg := "[" + user.Name + ":" + "Online...\n"
			u.SendMsg(onlineMsg)
		}
		u.server.mu.Unlock()
	} else if len(msg) > 7 && msg[:7] == "rename|" {
		newName := strings.Split(msg, "|")[1]
		_, ok := u.server.Users[newName]
		if ok {
			u.SendMsg("username is already taken\n")
		} else {
			u.server.mu.Lock()
			delete(u.server.Users, u.Name)
			u.server.Users[newName] = u
			u.server.mu.Unlock()

			u.Name = newName
			u.SendMsg("Username updated successfully: " + u.Name + "\n")
		}
	} else if len(msg) > 4 && msg[:3] == "to|" {
		remoteName := strings.Split(msg, "|")[1]
		if remoteName == "" {
			u.SendMsg("Invalid message format, expected: to|userName|message\n")
			return
		}

		remoteUser, ok := u.server.Users[remoteName]
		if !ok {
			u.SendMsg("User not found\n")
			return
		}

		content := strings.Split(msg, "|")[2]
		if content == "" {
			u.SendMsg("Message is empty, please resend\n")
			return
		}
		remoteUser.SendMsg(u.Name + " to you: " + content + "\n")
	} else {
		u.server.Broadcast(u, msg)
	}
}

func (u *User) ListenMessage() {
	for {
		msg := <-u.MsgCh
		u.conn.Write([]byte(msg + "\n"))
	}
}
