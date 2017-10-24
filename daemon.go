package main

import (
 "syscall"
 "os"
 "fmt"
)

func daemon (nochdir, noclose int) int {
    var ret uintptr
    var err syscall.Errno

    ret,_,err = syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
    if err != 0 { fmt.Print("Fork fail"); return -1 }
    switch (ret) {
        case 0:
            break
        default:
            os.Exit(0)
    }

    if pid,_:=syscall.Setsid(); pid == -1 { return -1 }
    if (nochdir == 0) { os.Chdir("/") }

    if noclose == 0 {
        f, e := os.Open("/dev/null")
        if e == nil {
            fd := f.Fd()
            syscall.Dup2( int(fd), int(os.Stdin.Fd()) )
            syscall.Dup2( int(fd), int(os.Stdout.Fd ()) )
            syscall.Dup2( int(fd), int(os.Stderr.Fd()) )
        }
    }

    return 0
}

