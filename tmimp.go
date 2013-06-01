package main

import "fmt"
import "encoding/xml"
import "encoding/json"
import "net/http"
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
  SerialNumber    string    `xml:"serial-number" json:"id"`
  RegistratioNumber  string `xml:"registration-number"`
  TransactionDate string `xml:"transaction-date" json:"TransactionDate_dt"`
  FilingDate string `xml:"case-file-header>filing-date" json:"FilingDate_dt"`
  StatusCode string `xml:"case-file-header>status-code"`
  StatusDate string `xml:"case-file-header>status-date" json:"StatusDate_dt"`
  MarkIdentification string `xml:"case-file-header>mark-identification"`
  MarkDrawingCode string `xml:"case-file-header>mark-drawing-code"`
  AttorneyName string `xml:"case-file-header>attorney-name"`
  CaseFileStatements []FileStatements `xml:"case-file-statements" json:"-"`
  FlatCaseFileStatements string `json:"caseFileStatements_t"`
  CaseFileEventStatements []FileEventStatements `xml:"case-file-event-statements" json:"-"`
  Classifications []Classification `xml:"classifications" json:"-"`
  FlatClassifications string `xml:"-" json:"classifications"`
  Correspondent []string `xml:"correspondent>address-"  json:"-"`
  FlatCorrespondent string `json:"correspondent"`
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

const BATCHSIZE = 1000
const WEBPOSTERS = 20
const XMLPROCESSORS = 1

// wait, aren't globals evil?
var inputFolder = flag.String("folder", "Downloads", "Input file folder path")
var inputLoud = flag.Bool("loud", false, "extra output informaiton")
var inputFake = flag.Bool("fake-it", false, "do everything but posting to the server")
var inputServer = flag.String("server", "http://localhost", "Where are we putting these records")

var status = make(chan string)
var post_capacitor = make(chan string)
var ready_to_ship = make(chan string, 5)
var case_files = make(chan string)
var wg sync.WaitGroup

func main() {
  flag.Parse()

  fmt.Println(runtime.GOMAXPROCS(runtime.NumCPU()))

  files, err := findTMFiles(*inputFolder)
  if err != nil {
    fmt.Println("Error reading folder:", err)
  }

  go keepTrack()
  for i := 0; i <= WEBPOSTERS; i++ {
    wg.Add(1)
    go toTheServer()
  }

  for i := 0; i <= XMLPROCESSORS; i++ {
    wg.Add(1)
    go processCaseFiles()
  }

  wg.Add(1)
  go bufferHttp()

  for _,name := range files {
    full_name := filepath.Join(*inputFolder, name)
    case_files <- full_name
  }
  close(case_files) // close the case_files channel, signalling a shutdown of the xmlParsers
  wg.Wait() // wait for all our goroutines to finish
}

// bundle postable json casefiles into arrays of BATCHSIZE elements
func bufferHttp() {
  var buffered_casefiles []string // they have already been converted to json stings by this point
  for {
    json := <-post_capacitor
    buffered_casefiles = append(buffered_casefiles, json)

    // This approach leaves scraps on the table
    // we need a signaliing mechanism for when all work is done
    // so we can push the remander to the server
    if len(buffered_casefiles) >= BATCHSIZE {

      if *inputLoud == true {
        fmt.Println("another batch ready to ship!")
      }
      status <- "batch"

      ready_to_ship <- strings.Join(buffered_casefiles, ",")

      buffered_casefiles = []string{}
    }
  }
}

func keepTrack() {
  total := 0
  finished := 0
  batched := 0
  sent := 0

  start := time.Now()

  var open_files []string

  if *inputLoud == false {
    painter := time.Tick(2 * time.Second)
    go func() {
      for now := range painter{
        os.Stdout.Write([]byte("\033[2J"))
        os.Stdout.Write([]byte("\033[0;0H"))
        fmt.Printf("total: \033[1m%d\033[0m complete:  \033[1m%d \033[0m", total, finished)
        fmt.Printf("total batched docs: \033[1m%d\033[0m posted docs:  \033[1m%d \033[0m\n", (batched * BATCHSIZE), (sent * BATCHSIZE))
        timer := now.Sub(start)
        fmt.Printf("\nticks: \033[1m%v\033[0m Processed Docs/Second:\033[1m%f\033[0m Posted Docs/Second:\033[1m%f\033[0m", timer, ( float64(finished) / timer.Seconds()), ( float64(sent * BATCHSIZE)  / timer.Seconds()))

        fmt.Println("\n\033[1mOpen Files\033[0m")
        for _,name := range open_files {
          fmt.Println(name)
        }
        fmt.Printf("\n\033[1mTotal Processors/Used:\033[0m %d/%d", runtime.NumCPU(), runtime.GOMAXPROCS(0))
      }

    }()
  }
  for {
    sig := <-status
    code := strings.Split(sig, ":")
    switch code[0] {
    case "open":
      open_files = append(open_files, code[1])
    case "add":
      total++
    case "pack":
      finished++
    case "batch":
      batched++
    case "sent":
      sent++
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

func processCaseFiles() {
  defer wg.Done() // don't hold execution closing after we are done
  for {
    file, more := <-case_files
    if more == false {
      break
    }
    processCaseFile(file)
  }
}

// processCaseFile extracts case_file information from the xml doc
func processCaseFile(file string) {

  xmlFile, err := os.Open(file)
  if err != nil {
    fmt.Println("Error opening file:", err)
    return
  }

  status <- ("open:" + file)
  defer xmlFile.Close()
  decoder := xml.NewDecoder(xmlFile)


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
        status <- "add:"
        fileStatements := ""
        fileClassifications := ""
        for _, fs := range cf.CaseFileStatements {
          fileStatements = fileStatements +  " " + fs.Text
        }
        for _, cl := range cf.Classifications {
          fileClassifications = fileClassifications +  " " + strings.Join(cl.UsCode, " ")
          fileClassifications = fileClassifications +  " " + strings.Join(cl.InternationalCode, " ")
        }
        cf.FlatClassifications = fileClassifications
        cf.FlatCaseFileStatements = fileStatements
        cf.FilingDate = dateFromTMString(cf.FilingDate)
        cf.TransactionDate = dateFromTMString(cf.TransactionDate)
        cf.StatusDate = dateFromTMString(cf.StatusDate)
        cf.FlatCorrespondent = strings.Join(cf.Correspondent, " ")
        go processCase(cf)
      }

    default:
    }

  }

  return
}

// this really should be in a supporting library as it's specific to Trademark data
//
// convert a TM date 20120101 to a standard data 2012-01-01
func dateFromTMString(tm_date string) (result string){
  runes := []rune(tm_date)

  //check for bad dates from the PTO
  if len(tm_date) < 8 {
    return
  }
  return string(runes[0:4]) + "-" + string(runes[4:6]) + "-" + string(runes[6:8])
}
// processCase converts a caseFile struct to a json string
func processCase(cf Casefile) {

  j, err := json.Marshal(cf)

  if err != nil {
    fmt.Println("Error marshalling json:", err)
    return
  }
  status <- "pack:"
  post_capacitor <- string(j)
}


// grab our batches of case files and send them to the server!
// this function monitors the shipping channel and dispatches posting jobs
func toTheServer() {
  defer wg.Done()

  for {
    casefile_set := <-ready_to_ship
    httpPost(casefile_set)
  }

}

func httpPost(j string) {
  body_array := []string{`{"docs":[`,string(j), "]}"}
  body := strings.Join(body_array, "")
  if *inputLoud == true {
    fmt.Println("About to POST:", string(body))
  }
  if *inputFake == false{

    client := &http.Client{}
    req, err := http.NewRequest("POST", *inputServer, strings.NewReader(body))

    req.Header.Add("Content-type", "application/json")

    req.SetBasicAuth("administrator", "foo")

    resp, err := client.Do(req)
    defer resp.Body.Close()
    if err != nil {
      fmt.Println("Error posting to LWBD:", err)
      status <-"sent:failed"
      return
    }
  }

  status <- "sent:"
}
