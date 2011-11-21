// Copyright 2011 Michael Schurter, BSD licensed
package main

import (
    "bufio"
    "flag"
    "log"
    "net"
    "os"
    "os/signal"
    "runtime"
    "runtime/pprof"
)

const BUFFER_SZ = 4096
const LOGGER_REOPEN = 1
const LOGGER_QUIT = 2

var sock_addr = flag.String("sock", "golog.sock", "Unix domain socket filename")
var log_filename = flag.String("log", "log.out", "Log file")
var maxprocs = flag.Int("procs", 1, "Processes to use")
var cpuprofile = flag.String("prof", "", "Profile CPU")

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
    sock, err := net.Listen("unixpacket", *sock_addr)
    defer os.Remove(*sock_addr)

    if err != nil {
        log.Fatalf("Error listening on socket:\n%v", err)
    }

    defer sock.Close()

    // Start logger goroutine w/a control chan & log chan
    logControlChan := make(chan int)
    logChan := make(chan []byte)
    go logger(logControlChan, logChan)

    // Wait for new connections
    log.Printf("Listening on %s:%s", sock.Addr().Network(), sock.Addr().String())
    go listen(sock, logChan)
    select {
    case <-signal.Incoming:
        log.Println("Sig")
        //TODO Check for SIGHUP and pass REOPEN to logger
    }
    log.Println("Done, exiting")
}

func listen(sock net.Listener, logChan chan []byte) {
    for {
        client, err := sock.Accept()
        if err != nil {
            if ne, ok := err.(net.Error); ok && ne.Temporary() {
                log.Printf("Error accepting client:\n%v", err)
            } else {
                // Non-temporary (fatal) error
                break
            }
        } else {
            go handle(client, logChan)
        }
    }
}

func handle(client net.Conn, logChan chan []byte) {
    buf := make([]byte, BUFFER_SZ)
    for {
        sz, err := client.Read(buf)
        if err != nil {
            log.Printf("Error reading from client %s:\n%v",
                    client.RemoteAddr().String(), err)
            break
        }
        logChan <- buf[:sz]
    }
    client.Close()
}


func openLog() (*os.File, *bufio.Writer) {
    // Open log file
    logf, err := os.OpenFile(
        *log_filename, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0664)
    if err != nil {
        log.Fatalf("Error opening file %s:\n%v", *log_filename, err)
    }
    buflogf := bufio.NewWriter(logf)
    return logf, buflogf
}

func logger(controlc chan int, logc chan []byte) {
    logf, logbuf := openLog()
    run := true
    for run {
        select {
        case data := <-logc:
            logbuf.Write(data)
            //FIXME This has to be the slowest way to append a newline
            logbuf.WriteString("\n")
        case sig := <-controlc:
            switch sig {
            case LOGGER_REOPEN:
                logf.Sync()
                logf.Close()
                logf, logbuf = openLog()
            case LOGGER_QUIT:
                run = false
            }
        }
    }
}
