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
)

func init() {
	xdebug=flag.Bool("debug", false, "enable debugging")
}

func main() {
	InitCfg()
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
	var xtype,xitem, mx, quota, ts string;
	var fmax, fquota float64;
	var last_ts, now int64;
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
		case xdata.ip_address!="":
			if (*xdebug) { fmt.Println("Using IP: ", xdata.ip_address) }
			xtype="I"
			xitem=xdata.ip_address
		default:
			return "REJECT no credentials"
	}
	err:=db.QueryRow("SELECT max, quota, unix_timestamp(ts) FROM "+cfg["policy_table"]+" where type=? and item=?",xtype, xitem).Scan(&mx, &quota, &ts)
	switch {
		case err==sql.ErrNoRows:
			if (*xdebug) { fmt.Println("NOT FOUND") }
			xlog.Info("New item: "+xtype+":"+xitem)
			fmax,_=strconv.ParseFloat(cfg["defaultquota"],64)
			fquota=0
			last_ts=time.Now().Unix()
			_,err=db.Exec("INSERT INTO "+cfg["policy_table"]+" set type=?, item=?, max=?, quota=0, ts=now()",xtype, xitem, cfg["defaultquota"])
			if (err!=nil) {
				xlog.Err(err.Error())
				if (*xdebug) { fmt.Println("ERROR INSERTING:",err.Error())}
			}
		case err!=nil:
			xlog.Err("ERROR: "+ err.Error())
			return "DUNNO"
		default:
			if (*xdebug) { fmt.Println("FOUND: ",mx, quota, ts) }
			fmax,_=strconv.ParseFloat(mx,64)
			fquota,_=strconv.ParseFloat(quota,64)
			last_ts,_=strconv.ParseInt(ts,10,64)
			if (*xdebug) { fmt.Println("DECODED: ",fmax, fquota, last_ts) }
	}
	now=time.Now().Unix()
	fquota=math.Max(0.0,fquota-(float64(now-last_ts)*fmax/3600.0)+1.0)
	if (*xdebug) { fmt.Println("NEW QUOTA: ", fquota) }
	if fquota>fmax {
			xlog.Info(fmt.Sprintf("DEFERRING overquota for item %s:%s [%.2f/%.2f]",xtype,xitem,fquota,fmax))
			return "DEFER quota exceeded"
	}
	_,err=db.Exec("UPDATE "+cfg["policy_table"]+" set quota=?, ts=now() where type=? and item=?",fquota, xtype, xitem)
	if (err!=nil) {
		xlog.Err(err.Error())
		if (*xdebug) { fmt.Println("ERROR UPDATING:",err.Error())}
	}
	xlog.Info(fmt.Sprintf("Updating quota for item %s:%s [%.2f/%.2f]. Sender: <%s>; Client IP: <%s>; SASL_username: <%s>", xtype, xitem, fquota, fmax, xdata.sender, xdata.ip_address, xdata.sasl_user ))
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
	db.Exec("set session TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	resp:=policy_verify(xdata, db)
	conn.Write([]byte(fmt.Sprintf("action=%s\n\n",resp)))
	conn.Close()
}
