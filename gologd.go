// Copyright 2011 Michael Schurter, BSD licensed
package main

import (
    "flag"
    "log"
    "net"
    "os"
    "os/signal"
    "runtime"
    "runtime/pprof"
    "sync"
)

const SIGHUP = 1

const NUM_BUFFERS = 100
const BUFFER_SZ = 4096
const LOGGER_REOPEN = 1
const LOGGER_QUIT = 2

var sock_addr = flag.String("sock", "golog.sock", "Unix domain socket filename")
var log_filename = flag.String("log", "log.out", "Log file")
var maxprocs = flag.Int("procs", 1, "Processes to use")
var cpuprofile = flag.String("prof", "", "Profile CPU")
var run = true
var wg = new(sync.WaitGroup)
var bufferQueue = make(chan []byte, NUM_BUFFERS)

func main() {
    flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }
    runtime.GOMAXPROCS(*maxprocs)
    // Open Socket
    os.Remove(*sock_addr)

    addr, err := net.ResolveUnixAddr("unixpacket", *sock_addr)
    if err != nil {
        log.Fatalf("Error resolving socket:\n%v", err)
    }

    sock, err := net.ListenUnix("unixpacket", addr)
    if err != nil {
        log.Fatalf("Error listening on socket:\n%v", err)
    }
    defer os.Remove(*sock_addr)
    defer sock.Close()

    // Fill buffer queue
    for i := 0; i < NUM_BUFFERS; i++ {
        bufferQueue <- make([]byte, BUFFER_SZ)
    }

    // Start logger goroutine w/a control chan & log chan
    logControlChan := make(chan int)
    logChan := make(chan []byte, 1000)
    go logger(logControlChan, logChan)

    // Wait for new connections
    log.Printf("Listening on %s:%s (%d)", sock.Addr().Network(), sock.Addr(), os.Getpid())
    go listen(sock, logChan)
    wg.Add(1)
    for run {
        select {
        case sig := <-signal.Incoming:
            signum := int32(sig.(os.UnixSignal))
            log.Printf("Signal: %s", sig)
            if signum == SIGHUP {
                logControlChan <- LOGGER_REOPEN
            } else {
                run = false
            }
        }
    }
    log.Println("Letting clients timeout...")
    wg.Wait()
    log.Println("Closing logger...")
    wg.Add(1)
    logControlChan <- LOGGER_QUIT
    wg.Wait()
    log.Println("Done, exiting")
}

func listen(sock *net.UnixListener, logChan chan []byte) {
    defer sock.Close()
    defer wg.Done()
    // Timeout after 2 seconds
    sock.SetTimeout(2e9)
    for run {
        client, err := sock.Accept()
        if err != nil {
            ne, ok := err.(net.Error)
            if !ok || !ne.Temporary() {
                // Non-temporary (fatal) error
                log.Printf("Error accepting client:\n%v", err)
                break
            }
        } else {
            wg.Add(1)
            go handle(client, logChan)
        }
    }
}

func handle(client net.Conn, logChan chan []byte) {
    defer client.Close()
    defer wg.Done()
    // Timeout after 2 seconds
    client.SetTimeout(2e9)
    for run {
        buf := <-bufferQueue
        sz, err := client.Read(buf)
        if err == os.EOF {
            log.Println("Client disconnected")
            break
        } else if err != nil {
            log.Printf("Error reading from client %s:\n%v",
                    client.RemoteAddr(), err)
            break
        }
        logChan <- buf[:sz]
    }
}


func openLog() *os.File {
    // Open log file
    logf, err := os.OpenFile(
        *log_filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
    if err != nil {
        log.Fatalf("Error opening file %s:\n%v", *log_filename, err)
    }
    return logf
}

func logger(controlc chan int, logc chan []byte) {
    logf := openLog()
    defer logf.Sync()
    defer logf.Close()
    defer wg.Done()
    newline := ([]byte("\n"))[0]
    for run {
        select {
        case data := <-logc:
            logf.Write(append(data, newline))
            bufferQueue <-data
        case sig := <-controlc:
            switch sig {
            case LOGGER_REOPEN:
                logf.Sync()
                logf.Close()
                logf = openLog()
                log.Printf("Reopened log file: %s", *log_filename)
            case LOGGER_QUIT:
                break
            }
        }
    }
}
