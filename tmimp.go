package main

import "fmt"
import "encoding/xml"
//import "encoding/json"
import "flag"
import "os"
import "io/ioutil"
import "path/filepath"
import "sync"
import "strings"
import "runtime"
import "time"

type Casefile struct {
  XMLName xml.Name `xml:"case-file" json:"-"`
  SerialNumber    string    `xml:"serial-number" json:"serial_number`
  RegistratioNumber  string `xml:"registration-number"`
  TransactionDate string `xml:"transaction-date"`
  FilingDate string `xml:"case-file-header>filing-date"`
  StatusCode string `xml:"case-file-header>status-code"`
  StatusDate string `xml:"case-file-header>status-date"`
  MarkIdentification string `xml:"case-file-header>mark-identification"`
  MarkDrawingCode string `xml:"case-file-header>mark-drawing-code"`
  AttorneyName string `xml:"attorney-name"`
  CaseFileStatements []FileStatements `xml:"case-file-statements"`
  CaseFileEventStatements []FileEventStatements `xml:"case-file-event-statements"`
  Classifications []Classification `xml:"classifications"`
  Correxpondent []string `xml:correspondent>address-`
}

type FileStatements struct {
  TypeCode string `xml:"case-file-statement>type-code"`
  Text string `xml:"case-file-statement>text"`
}

type FileEventStatements struct {
  Code string `xml:"case-file-event-statement>code"`
  EventType string `xml:"case-file-event-statement>type"`
  DescriptionText string `xml:"case-file-event-statement>description-text"`
  Date string `xml:"case-file-event-statement>date"`
  Number int `xml:"case-file-event-statement>number"`
}

type Classification struct {
  InternationalCodeTotalNo int `xml:"classification>international-code-total-no"`
  UsCodeTotalNo int `xml:"classification>us-code-total-no"`
  InternationalCode []string `xml:"classification>international-code"`
  UsCode []string `xml:"classification>us-code"`
  StatusCode string `xml:"classification>status-code"`
  StatusDate string `xml:"classification>status-date"`
  FirstUseAnywhereDate string `xml:"classification>first-use-anywhere-date"`
  FirstUseInCommerceDate string `xml:"classification>first-use-in-commerce-date"`
  PrimaryCode string `xml:"classification>primary-code"`
}



// wait, aren't globals evil?
var inputFile = flag.String("infile", "enwiki-latest-pages-articles.xml", "Input file path")
var inputFolder = flag.String("folder", "Downloads", "Input file folder path")
var status = make(chan string, 100)

func main() {
  flag.Parse()

  fmt.Println(runtime.GOMAXPROCS(runtime.NumCPU()))
  //total := processCaseFile(*inputFile)
  //fmt.Printf("Total cases: %d \n", total)
  files, err := findTMFiles(*inputFolder)
  if err != nil {
    fmt.Println("Error reading folder:", err)
  }

  var waitGroup sync.WaitGroup

  go keepTrack()

  for _,name := range files {
    full_name := filepath.Join(*inputFolder, name)
    fmt.Println("Processing file:", full_name)
    waitGroup.Add(1)
    go processCaseFile(full_name, &waitGroup)
  }

  waitGroup.Wait() // wait for all our goroutines to finish
}

func keepTrack() {
  total := 0
  finished := 0
  start := time.Now()

  var open_files []string

  painter := time.Tick(1 * time.Second)
  go func() {
    for now := range painter{
      os.Stdout.Write([]byte("\033[2J"))
      os.Stdout.Write([]byte("\033[0;0H"))
      fmt.Printf("total: \033[1m%d\033[0m complete:  \033[1m%d \033[0m", total, finished)
      fmt.Printf("\nticks: \033[1m%v\033[0m Docs/Second:\033[1m%f\033[0m", now.Sub(start), ( float64(finished) / now.Sub(start).Seconds()))

      fmt.Println("\n\033[1mOpen Files\033[0m")
      for _,name := range open_files {
        fmt.Println(name)
      }
      fmt.Printf("\n\033[1mTotal Processors/Used:\033[0m %d/%d", runtime.NumCPU(), runtime.GOMAXPROCS(0))
    }

  }()
  for {
    sig := <-status
    code := strings.Split(sig, ":")
    switch code[0] {
    case "open":
      open_files = append(open_files, code[1])
    case "add":
      total++
    case "fin":
      finished++
    default:
      fmt.Println("Unknown status:", sig)
    }


  }
}
func findTMFiles(folder string) (files []string, err error) {
  fileinfo, err := ioutil.ReadDir(folder)

  if err != nil {
    fmt.Printf("Folder '%s' could not be opened: %+v", folder, err)
    return
  }

  for _, file := range fileinfo {
    if strings.HasSuffix(file.Name(), ".xml") {
      files = append(files, file.Name())
    }
  }
  return
}

func processCaseFile(file string, waitGroup *sync.WaitGroup) (total int) {
  defer waitGroup.Done()

  xmlFile, err := os.Open(file)
  if err != nil {
    fmt.Println("Error opening file:", err)
    return
  }

  status <- ("open:" + file)
  defer xmlFile.Close()
  decoder := xml.NewDecoder(xmlFile)


  total = 0

  var inElement string

  for {

    t, _ := decoder.Token()
    if t == nil {
      break
    }

    switch se := t.(type) {

    case xml.StartElement:
      inElement = se.Name.Local

      if inElement == "case-file" {
        var cf Casefile
        err := decoder.DecodeElement(&cf, &se)
        if err != nil {
          fmt.Printf("Error opening file:", err)
        }
        total++
        status <- "add:"
        waitGroup.Add(1)
        go processCase(cf, waitGroup, status)
      }

    default:
    }

  }

  return
}

func processCase(cf Casefile, wg *sync.WaitGroup, status chan string) {
  defer wg.Done() // we are keeping track of how many of these are run so we don't exit early
  
  //j, err := json.Marshal(cf)

  //if err != nil {
    //fmt.Println("Error opening file:", err)
  //}
  //fmt.Printf("casefile as json: \n %s \n", j)
  status <- "fin:"
}
