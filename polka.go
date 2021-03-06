package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"bufio"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"math"
	"strconv"
//	"log"
	"log/syslog"
	"flag"
	"sync"
)

type connData struct {
	sasl_user  string;
	ip_address string;
	sender     string;
	rcpt       string;
}

var (
	xlog *syslog.Writer
	xdebug *bool
	xmutex sync.Mutex
)

func init() {
	xdebug=flag.Bool("debug", false, "enable debugging")
}

func main() {
	InitCfg()
	InitReputation()
	flag.Parse()
	if (!*xdebug) { daemon(0,0) } else {fmt.Println("Starting in debug mode")}
	// Listen for incoming connections.
	l, err := net.Listen("tcp", cfg["bind"]+":"+cfg["port"])
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	
	// open connection to the database
	db,err:=sql.Open("mysql",fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?autocommit=true",cfg["dbuser"],cfg["dbpass"],cfg["dbhost"],cfg["dbport"],cfg["dbname"]))
	if (err!=nil) {
		fmt.Println("ERROR CONNECTING MYSQL")
		os.Exit(1)
	}
	defer db.Close()
	
	xlog, _ = syslog.New(syslog.LOG_INFO|syslog.LOG_MAIL, "POLKA")
	
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
		xlog.Err("Error accepting: "+ err.Error())
		os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn, db)
	}
}

func sender_permitted(xdata connData, db *sql.DB) bool {
	if xdata.sender=="" {
		return true
	}
	var cnt=0
	err:=db.QueryRow("SELECT count(sender) FROM "+cfg["badmailfrom_table"]+" where ? like sender",xdata.sender).Scan(&cnt)
	if err!=nil {
		xlog.Err("ERROR: "+ err.Error())
		return true
	}
	return cnt==0
}

func recipient_permitted(xdata connData, db *sql.DB) bool {
	if xdata.rcpt=="" {
		return true
	}
	var cnt=0
	err:=db.QueryRow("SELECT count(rcpt) FROM "+cfg["badmailto_table"]+" where ? like rcpt",xdata.rcpt).Scan(&cnt)
	if err!=nil {
		xlog.Err("ERROR: "+ err.Error())
		return true
	}
	return cnt==0
}

func policy_verify(xdata connData, db *sql.DB) string {
	var xtype,xitem, mx, quota, ts, s_now string;
	var fmax, fquota float64;
	var last_ts, i_now int64;
	sender_accounting,err_a :=strconv.ParseInt(cfg["sender_accounting"],10,8)
	if err_a!=nil { 
		sender_accounting=0
	}
		
	if !sender_permitted(xdata,db) {
		xlog.Info("Rejecting bad sender "+xdata.sender)
		return "REJECT bad sender"
	}
	if !recipient_permitted(xdata,db) {
		xlog.Info("Rejecting bad recipient "+xdata.rcpt)
		return "REJECT bad recipient"
	}
	switch {
		case xdata.sasl_user!="":
			if (*xdebug) { fmt.Println("Using username: ", xdata.sasl_user) }
			xtype="U"
			xitem=xdata.sasl_user
		case xdata.sender!="" && sender_accounting==1:
			if (*xdebug) { fmt.Println("Using Sender: ", xdata.sender) }
			xtype="S"
			xitem=xdata.sender
		case xdata.ip_address!="":
			if (*xdebug) { fmt.Println("Using IP: ", xdata.ip_address) }
			xtype="I"
			xitem=xdata.ip_address
		default:
			return "REJECT no credentials"
	}
	var repu_logmsg string
	var repu_ok bool
	if repu_ok, repu_logmsg=verify_reputation(xdata.sender, xdata.ip_address); !repu_ok {
		return "REJECT bad reputation"
	}
	xmutex.Lock()
	defer xmutex.Unlock()
	tx,err:=db.Begin()
	if err!=nil {
		xlog.Err("TRANSACTION ERROR:"+err.Error())
		if (*xdebug) { fmt.Println("ERROR STARTING TRANSACTION:",err.Error())}
		return "DUNNO"
	}
	defer tx.Commit()
	err=tx.QueryRow("SELECT max, quota, unix_timestamp(ts), unix_timestamp(now()) FROM "+cfg["policy_table"]+" where type=? and item=? FOR UPDATE",xtype, xitem).Scan(&mx, &quota, &ts, &s_now)
	switch {
		case err==sql.ErrNoRows:
			if (*xdebug) { fmt.Println("NOT FOUND") }
			xlog.Info("New item: "+xtype+":"+xitem)
			fmax,_=strconv.ParseFloat(cfg["defaultquota"],64)
			fquota=0
			last_ts=time.Now().Unix()
			i_now=time.Now().Unix()
			_,err=tx.Exec("INSERT INTO "+cfg["policy_table"]+" set type=?, item=?, max=?, quota=0, ts=now()",xtype, xitem, cfg["defaultquota"])
			if (err!=nil) {
				xlog.Err("INSERT ERROR:"+err.Error())
				if (*xdebug) { fmt.Println("ERROR INSERTING:",err.Error())}
				return "DUNNO"
			}
		case err!=nil:
			xlog.Err("ERROR: "+ err.Error())
			return "DUNNO"
		default:
			if (*xdebug) { fmt.Println("FOUND: ",mx, quota, ts) }
			fmax,_=strconv.ParseFloat(mx,64)
			fquota,_=strconv.ParseFloat(quota,64)
			last_ts,_=strconv.ParseInt(ts,10,64)
			i_now,_=strconv.ParseInt(s_now,10,64)
			if (*xdebug) { fmt.Println("DECODED: ",fmax, fquota, last_ts) }
	}
	delta_t:=i_now-last_ts
	if (*xdebug) { fmt.Println("DeltaT: ",delta_t) }
	fquota=math.Max(0.0,fquota-(float64(delta_t)*fmax/3600.0)+1.0)
	if (*xdebug) { fmt.Println("NEW QUOTA: ", fquota) }
	if fquota>fmax {
			xlog.Info(fmt.Sprintf("DEFERRING overquota for item %s:%s [%.2f/%.2f]",xtype,xitem,fquota,fmax))
			return "DEFER quota exceeded"
	}
	_,err=tx.Exec("UPDATE "+cfg["policy_table"]+" set quota=?, ts=now() where type=? and item=?",fquota, xtype, xitem)
	if (err!=nil) {
		xlog.Err("UPDATE ERROR"+err.Error())
		if (*xdebug) { fmt.Println("ERROR UPDATING:",err.Error())}
		return "DUNNO"
	}
	logmsg:=fmt.Sprintf("Updating quota for item %s:%s [%.2f/%.2f]. Sender: <%s>; Client IP: <%s>; SASL_username: <%s>", xtype, xitem, fquota, fmax, xdata.sender, xdata.ip_address, xdata.sasl_user )
	if repu_logmsg!=""  { logmsg+=repu_logmsg }
	xlog.Info(logmsg)
	return "DUNNO"  // not OK so we can pipe more checks in postfix
	
}

// Handles incoming requests.
func handleRequest(conn net.Conn, db *sql.DB) {
	var xdata connData;
	reader:=bufio.NewReader(conn)
	for {
		s,err:=reader.ReadString('\n')
		if err!=nil {
			fmt.Println("Error reading:", err.Error())
			break
		}
		s=strings.Trim(s," \n\r")
		s=strings.ToLower(s)
		if (s=="")  { break; }
		vv:=strings.SplitN(s,"=",2)
		if len(vv)<2 {
			fmt.Println("Error processing line")
			continue
			}
		// if (*xdebug) { fmt.Println("..", s, ":", vv[0],"->",vv[1]) }
		vv[0]=strings.Trim(vv[0]," \n\r")
		vv[1]=strings.Trim(vv[1]," \n\r")
		switch vv[0] {
			case "sasl_username": xdata.sasl_user=vv[1]
			case "client_address": xdata.ip_address=vv[1]
			case "sender": xdata.sender=vv[1]
			case "recipient": xdata.rcpt=vv[1]
		}
	}
	db.Ping()
	db.Exec("set time_zone='+00:00'") // timezone UTC
	//db.Exec("set session TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	db.Exec("set session TRANSACTION ISOLATION LEVEL READ COMMITTED")
	resp:=policy_verify(xdata, db)
	conn.Write([]byte(fmt.Sprintf("action=%s\n\n",resp)))
	conn.Close()
}
