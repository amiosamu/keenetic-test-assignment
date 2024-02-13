package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func getPort(args []string) (int, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("port isn't provided via command line args")
	}

	port, err := strconv.Atoi(args[0])
	if err != nil {
		return 0, fmt.Errorf("it is not possible to convert the %s parameter to a number", args[0])
	}

	if port <= 0 || port > 65535 {
		return 0, fmt.Errorf("port number must be in range from 0 to 65535")
	}
	return port, err
}

type Queue struct {
	elements []string
	queueCh  []chan string
	mutex    sync.Mutex
}

func (q *Queue) Enqueue(v string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.queueCh) == 0 {
		q.elements = append(q.elements, v)
		return
	}
	ch := q.queueCh[0]
	q.queueCh = q.queueCh[1:]
	ch <- v
}

func (q *Queue) Dequeue(ch chan string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.elements) > 0 {
		v := q.elements[0]
		q.elements = q.elements[1:]
		ch <- v
	}
	q.queueCh = append(q.queueCh, ch)
}

func (q *Queue) RemoveCh(ch chan string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for idx, sCh := range q.queueCh {
		if sCh == ch {
			q.queueCh = append(q.queueCh[:idx], q.queueCh[idx+1:]...)
			return
		}
	}
}

type Server struct {
	queue  map[string]*Queue
	server *http.Server
	chErr  chan error
	mutex  sync.Mutex
}

func NewQeueue() *Queue {
	res := Queue{
		elements: make([]string, 0),
		queueCh:  make([]chan string, 0),
	}

	return &res

}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PUT":
		s.handlePut(w, r)
	case "GET":
		s.handleGet(w, r)
	default:
		w.WriteHeader(400)
	}
}

func (s *Server) getQueue(name string) *Queue {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	q, ok := s.queue[name]

	if !ok {
		q = NewQeueue()
		s.queue[name] = q
	}

	return q
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[1:]

	if len(path) == 0 {
		w.WriteHeader(400)
		return
	}

	v := r.URL.Query().Get("v")
	if len(v) == 0 {
		w.WriteHeader(400)
		return
	}

	q := s.getQueue(path)
	q.Enqueue(v)
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[1:]

	if len(path) == 0 {
		w.WriteHeader(400)
		return
	}

	timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
	if err != nil {
		timeout = 0
	}

	res := make(chan string, 1)

	q := s.getQueue(path)

	q.Dequeue(res)

	defer q.RemoveCh(res)

	select {
	case v := <-res:
		w.WriteHeader(200)
		w.Write([]byte(v))

	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(404)
	case <-r.Context().Done():
		break
	}
}

func NewServer(port int) *Server {
	res := &Server{
		queue: make(map[string]*Queue),
		server: &http.Server{
			Addr: fmt.Sprintf(":%d", port),
		},
	}
	res.server.Handler = res
	return res
}

func (s *Server) Serve() (chan error, error) {
	ch := make(chan error)

	ln, err := net.Listen("tcp", s.server.Addr)

	if err != nil {
		return ch, err
	}

	defer ln.Close()

	go func() {
		ch <- s.server.Serve(ln)
	}()

	s.chErr = ch

	return ch, nil
}

func (s *Server) Shutdown(ctx context.Context) {
	s.server.Shutdown(ctx)
}

func (s *Server) EventLoop() {
	<-s.chErr
}

func (s *Server) setupInterrupt(cancel context.CancelFunc, ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}()
}

func main() {
	port, err := getPort(os.Args[1:])

	if err != nil {
		fmt.Fprintf(os.Stderr, "error:%v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	server := NewServer(port)

	_, err = server.Serve()

	if err != nil {
		fmt.Fprintf(os.Stderr, "error:%v\n", err)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error:%v\n", err)
		os.Exit(1)
	}

	server.setupInterrupt(cancel, ctx)

	fmt.Printf("Server started at 0.0.0.0:%d\n", port)

	server.EventLoop()
	fmt.Println("Exit...")

}
