package resumable

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const (
	stopped = 0
	paused  = 1
	running = 2
)

// WG exports wait group, so we can wait for it
var DefaultWG sync.WaitGroup

type Logger struct {
	ErrorLog *log.Logger
	InfoLog  *log.Logger
	DebugLog *log.Logger
}

// Resumable structure
type Resumable struct {
	client    *http.Client
	method    string
	url       string
	filePath  string
	id        string
	chunkSize int
	file      *os.File
	channel   chan int
	Status    UploadStatus
	logger    Logger
	wg        *sync.WaitGroup
}

// UploadStatus holds the data about upload
type UploadStatus struct {
	Size                 int64
	SizeTransferred      int64
	Parts                uint64
	PartsTransferred     uint64
	IsDone               bool
	TransferredException bool
}

// New creates new instance of resumable Client
func New(url string, filePath string, client *http.Client, chunkSize int, debug bool) *Resumable {
	return NewResumable("POST", url, filePath, client, chunkSize, nil, &DefaultWG)
}

// New creates new instance of resumable Client
func NewResumable(method string, url string, filePath string, client *http.Client, chunkSize int,
	logger *Logger,
	wg *sync.WaitGroup) *Resumable {
	wgObj := wg
	if wgObj == nil {
		wgObj = new(sync.WaitGroup)
	}

	if logger == nil {
		logger = &Logger{
			DebugLog: log.New(NewNullWriter(), "DEBUG\t", log.Ldate|log.Ltime|log.Lshortfile),
			InfoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
			ErrorLog: log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
		}
	}

	resumable := &Resumable{
		client:    client,
		method:    method,
		url:       url,
		filePath:  filePath,
		id:        generateSessionID(),
		chunkSize: chunkSize,
		wg:        wgObj,
		logger:    *logger,
		Status: UploadStatus{
			Size:                 0,
			SizeTransferred:      0,
			Parts:                0,
			PartsTransferred:     0,
			IsDone:               false,
			TransferredException: false,
		},
	}

	return resumable
}

// Init method initializes upload
func (c *Resumable) Init() error {
	fileStat, err := os.Stat(c.filePath)
	if c.checkError(err) {
		return err
	}

	c.Status.Size = fileStat.Size()
	c.Status.Parts = uint64(math.Ceil(float64(c.Status.Size) / float64(c.chunkSize)))

	c.channel = make(chan int, 1)
	c.file, err = os.Open(c.filePath)
	if c.checkError(err) {
		return err
	}
	c.wg.Add(1)

	go func() {
		defer c.Close()
		defer c.wg.Done()
		c.upload()
		c.logger.DebugLog.Printf("Upload goroutine done\n")
	}()

	c.logger.InfoLog.Printf("Done\n")
	return nil
}

func (c *Resumable) Close() {
	c.logger.DebugLog.Printf("Close file %s\n", c.filePath)
	err := c.file.Close()
	if err != nil {
		c.logger.ErrorLog.Println(err)
	}
}

// Start set upload state to uploading
func (c *Resumable) Start() error {
	c.channel <- running
	return nil
}

// Pause set upload state to paused
func (c *Resumable) Pause() error {
	if c.Status.IsDone {
		return fmt.Errorf("upload file is done. Can not pause process")
	}
	c.channel <- paused
	return nil
}

// Cancel set upload state to stopped
func (c *Resumable) Cancel() error {
	if c.Status.IsDone {
		return fmt.Errorf("upload file is done. Can not cancel process")
	}
	c.channel <- stopped
	return nil
}

func (c *Resumable) checkError(err error) bool {
	if err != nil {
		c.logger.ErrorLog.Println(err)
		c.uploadDone(true)
	}
	return err != nil
}

func (c *Resumable) upload() {
	state := paused
	i := uint64(0)

	for !c.Status.IsDone {
		select {
		case state = <-c.channel:
			switch state {
			case stopped:
				c.logger.DebugLog.Printf("Upload %s: stopped\n", c.id)
				return
			case running:
				if c.Status.PartsTransferred > 0 {
					i = i - 1
				}

				c.logger.DebugLog.Printf("Upload %s: running\n", c.id)

			case paused:
				c.logger.DebugLog.Printf("Upload %s: paused\n", c.id)
			}

		default:
			runtime.Gosched()
			if state == paused {
				break
			}

			c.uploadChunk(i)
			i = i + 1
		}
	}
}

func (c *Resumable) GetWG() sync.WaitGroup {
	return *c.wg
}

type CalculateTransferredSize func(body string, partSize int, status UploadStatus) (int64, error)

func calculateTransferredSize(body string, partSize int, status UploadStatus) (int64, error) {
	if body != "" {
		return parseBody(body)
	}

	result := int64(partSize)
	if (result + status.SizeTransferred) > status.Size {
		result = status.Size - status.SizeTransferred
	}

	return result, nil
}

func parseBody(body string) (int64, error) {
	fromTo := strings.Split(body, "/")[0]
	splitted := strings.Split(fromTo, "-")

	partTo, err := strconv.ParseInt(splitted[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return partTo, nil
}

func (c *Resumable) uploadDone(isException bool) {
	if isException {
		c.logger.ErrorLog.Printf("Upload process done by exception\n")
	} else {
		c.logger.InfoLog.Printf("Upload process done\n")
	}
	c.Status.IsDone = true
	c.Status.TransferredException = isException
}

func (c *Resumable) uploadChunk(i uint64) {
	if i == c.Status.Parts {
		c.logger.InfoLog.Printf("Upload %s: done\n", c.id)
		c.uploadDone(false)
	} else if c.Status.TransferredException {
		c.logger.ErrorLog.Printf("ERROR. Transfered exception\n")
	} else {
		fileName := filepath.Base(c.filePath)
		partSize := int(math.Ceil((math.Min(float64(c.chunkSize), float64(c.Status.Size-int64(i*uint64(c.chunkSize)))))))
		if partSize <= 0 {
			return
		}

		partBuffer := make([]byte, partSize)
		readBytes, err := c.file.Read(partBuffer)
		if err != nil {
			c.logger.ErrorLog.Println(err)
			c.uploadDone(true)
			return
		}
		c.logger.DebugLog.Printf("Read %d bytes", readBytes)

		contentRange := generateContentRange(i, c.chunkSize, partSize, c.Status.Size)

		var isSuccess = false
		var responseBody = ""
		var errorCount = 0

		for !isSuccess && errorCount < 3 {
			isSuccess, responseBody, err = httpRequest(c.method, c.url, c.client, c.id, partBuffer, contentRange, fileName, c.logger.DebugLog)
			c.logger.DebugLog.Printf("isSuccess: %t \n", isSuccess)
			if err != nil {
				c.logger.ErrorLog.Println(err)
				isSuccess = false
			}
			if !isSuccess {
				errorCount++
			}
		}

		if isSuccess {
			transferredBytes, err1 := calculateTransferredSize(responseBody, partSize, c.Status)
			if !c.checkError(err1) {
				c.Status.SizeTransferred += transferredBytes
				c.Status.PartsTransferred = i + 1
			}
		} else {
			c.uploadDone(true)
		}

		c.logger.DebugLog.Printf("Part: %d of: %d", c.Status.PartsTransferred, c.Status.Parts)
	}
}

func httpRequest(method string,
	url string,
	client *http.Client,
	sessionID string,
	part []byte,
	contentRange string,
	fileName string,
	debugLogger *log.Logger) (bool, string, error) {
	request, err := http.NewRequest(method, url, bytes.NewBuffer(part))
	if err != nil {
		return false, "", err
	}

	request.Header.Add("Content-Type", "application/octet-stream")
	request.Header.Add("Content-Disposition", "attachment; filename=\""+fileName+"\"")
	request.Header.Add("Content-Range", contentRange)
	request.Header.Add("Session-ID", sessionID)

	response, err := client.Do(request)
	if err != nil {
		return false, "", err
	}

	statusCode := response.StatusCode

	debugLogger.Printf("  %s HTTP code %d", contentRange, statusCode)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return false, "", err
	}
	debugLogger.Printf("  Body %v\n", body)
	return statusCode >= 200 && statusCode <= 299, string(body), nil
}
