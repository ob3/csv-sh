package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"github.com/rs/xid"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main(){



	csvFile := flag.String("csv", "", "/path/to/csv/file.csv")
	command := flag.String("cmd", "", "\"echo {{csv_field_name}}\"")
	workerCount := flag.Int("worker", 100, "default 100")

	flag.Parse()
	if len(*csvFile) == 0 || len(*command) == 0 {
		println("use -h for usage")
		os.Exit(1)
	}

	setUpLog()
	reader := getCsv(*csvFile)
	title := getTitle(reader)
	pool := grpool.NewPool(*workerCount, 50)
	defer pool.Release()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		constructedCommand := constructCommand(*command, title, record)
		pool.JobQueue <- func() { execute(constructedCommand) }
	}
	pool.WaitAll()
	log.Info("finished")

}

func constructCommand(command string, title []string, record []string) string {

	for idx, element := range title {
		command = strings.Replace(command, "{{"+ strings.TrimSpace(element)+"}}", strings.TrimSpace(record[idx]), -1)
	}
	return command
}


func getTitle(reader *csv.Reader) []string{
	title, err := reader.Read()

	if err != nil {
		log.Error(err)
		panic(err)
	}
	return title
}

func setUpLog(){
	t := time.Now()
	timestamp := (t.Format(time.RFC3339))
	logName := "csv-commandline-"+timestamp+".log"
	logFile, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Error(err)
		panic(err)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
}

func getCsv(file string) *csv.Reader{
	csvFile, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	return csv.NewReader(bufio.NewReader(csvFile))
}

func getTemporaryFile(cmd string) string{
	guid := xid.New().String()
	fileName := "/tmp/"+guid+".sh"
	err := ioutil.WriteFile(fileName, []byte(cmd), 0700)
	check(err)
	return fileName
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func execute(fullCommand string){
	fullCommandArray := strings.Split(fullCommand, " ")
	shFile := getTemporaryFile(fullCommand)
	defer os.Remove(shFile)

	executor := exec.Command("sh", shFile)
	out, err := executor.Output()

	if err != nil {
		log.Error(err)
		log.Error("execute: ", fullCommand, " ",  string(out))
	}else{
		log.Info(fullCommandArray, " --> ", string(out))
	}

}
