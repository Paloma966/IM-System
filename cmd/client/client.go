package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	conn       net.Conn
	flag       int
}

func NewClient(serverIP string, serverPort int) *Client {
	client := &Client{
		ServerIP:   serverIP,
		ServerPort: serverPort,
		flag:       999,
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverIP, serverPort))
	if err != nil {
		fmt.Println("net.Dial err:", err)
		return nil
	}
	client.conn = conn

	return client
}

func (c *Client) HandleResponse() {
	io.Copy(os.Stdout, c.conn)
}

func (c *Client) menu() bool {
	var flag int

	fmt.Println("1. Group chat")
	fmt.Println("2. Private chat")
	fmt.Println("3. Update username")
	fmt.Println("0. Exit")

	fmt.Scanln(&flag)

	if flag >= 0 && flag <= 3 {
		c.flag = flag
		return true
	} else {
		fmt.Println("Invalid option")
		return false
	}
}

func (c *Client) SelectUsers() {
	sendMsg := "who\n"
	_, err := c.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("conn Write err:", err)
		return
	}
}

func (c *Client) PrivateChat() {
	var remoteName string
	var chatMsg string
	c.SelectUsers()

	fmt.Println(">>>> Please enter chat content[username]")
	fmt.Scanln(&remoteName)

	for remoteName != "exit" {
		fmt.Println(">>>> Please enter chat content")
		fmt.Scanln(&chatMsg)
		for chatMsg != "exit" {
			if len(chatMsg) != 0 {
				sendMsg := "to|" + remoteName + "|" + chatMsg + "\n\n"
				_, err := c.conn.Write([]byte(sendMsg))
				if err != nil {
					fmt.Println("conn Write err:", err)
					break
				}
			}

			chatMsg = ""
			fmt.Println(">>>> Please enter chat content")
			fmt.Scanln(&chatMsg)
		}
		c.SelectUsers()
		fmt.Println(">>>> Please enter chat content[username]")
		fmt.Scanln(&remoteName)
	}
}

func (c *Client) PublicChat() {
	var chatMsg string
	fmt.Println(">>>> Please enter chat content")
	fmt.Scanln(&chatMsg)

	for chatMsg != "exit" {
		if len(chatMsg) != 0 {
			sendMsg := chatMsg + "\n"
			_, err := c.conn.Write([]byte(sendMsg))
			if err != nil {
				fmt.Println("conn Write err:", err)
				break
			}
		}

		chatMsg = ""
		fmt.Println(">>>> Please enter chat content")
		fmt.Scanln(&chatMsg)
	}
}

func (c *Client) UpdateName() bool {
	fmt.Println(">>>> Please enter username:")
	fmt.Scanln(&c.Name)

	sendMsg := "rename|" + c.Name + "\n"
	_, err := c.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("conn.Write err:", err)
		return false
	}
	return true
}

func (c *Client) Run() {
	for c.flag != 0 {
		for c.menu() != true {
		}
		switch c.flag {
		case 1:
			c.PublicChat()
		case 2:
			c.PrivateChat()
		case 3:
			c.UpdateName()
		}
	}
}

var serverIP string
var serverPort int

func init() {
	flag.StringVar(&serverIP, "ip", "127.0.0.1", "server IP address")
	flag.IntVar(&serverPort, "port", 8888, "server port")
}

func main() {
	flag.Parse()

	client := NewClient(serverIP, serverPort)
	if client == nil {
		fmt.Println(">>>> Failed to connect to server...")
		return
	}

	go client.HandleResponse()

	fmt.Println(">>>> Connect to server...")

	client.Run()
}
