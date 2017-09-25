package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/goadapp/goad/api"
	"github.com/goadapp/goad/infrastructure/aws/sqs"
	"github.com/goadapp/goad/version"
	"github.com/streadway/amqp"
)

import "math/rand"
import "math/big"
import crand "crypto/rand"


var (
	app = kingpin.New("goad-lambda", "Utility deployed into aws lambda by goad")

	address        = app.Arg("url", "URL to load test").Required().String()
	address2        = app.Arg("content_server", "URL to content server").Required().String()
	requestMethod  = app.Flag("method", "HTTP method").Short('m').Default("GET").String()
	requestBody    = app.Flag("body", "HTTP request body").Short('b').String()
	requestHeaders = app.Flag("header", "List of headers").Short('H').Strings()

	awsRegion   = app.Flag("aws-region", "AWS region to run in").Short('r').String()
	queueRegion = app.Flag("queue-region", "SQS queue region").Short('q').String()
	sqsURL      = app.Flag("sqsurl", "SQS URL").String()

	clientTimeout      = app.Flag("client-timeout", "Request timeout duration").Short('s').Default("15s").Duration()
	reportingFrequency = app.Flag("frequency", "Reporting frequency in seconds").Short('f').Default("15s").Duration()

	concurrencyCount              = app.Flag("concurrency", "Number of concurrent requests").Short('c').Default("10").Int()
	maxRequestCount               = app.Flag("requests", "Total number of requests to make").Short('n').Default("10").Int()
	previousCompletedRequestCount = app.Flag("completed-count", "Number of requests already completed in case of lambda timeout").Short('p').Default("0").Int()
	execTimeout                   = app.Flag("execution-time", "Maximum execution time in seconds").Short('t').Default("0").Int()
	runnerID                      = app.Flag("runner-id", "A id to identifiy this lambda function").Required().Int()
)

const AWS_MAX_TIMEOUT = 250

func main() {
	nBig, _ := crand.Int(crand.Reader, big.NewInt(27))
	value := nBig.Int64()
	rand.Seed(value)

	lambdaSettings := parseLambdaSettings()
	Lambda := newLambda(lambdaSettings)
	Lambda.runLoadTest()
}

func parseLambdaSettings() LambdaSettings {
	app.HelpFlag.Short('h')
	app.Version(version.String())
	kingpin.MustParse(app.Parse(os.Args[1:]))

	requestParameters := requestParameters{
		URL:            *address,
		ContentServer:  *address2,
		RequestHeaders: *requestHeaders,
		RequestMethod:  *requestMethod,
		RequestBody:    *requestBody,
	}

	lambdaSettings := LambdaSettings{
		ClientTimeout:         *clientTimeout,
		SqsURL:                *sqsURL,
		MaxRequestCount:       *maxRequestCount,
		CompletedRequestCount: *previousCompletedRequestCount,
		ConcurrencyCount:      *concurrencyCount,
		QueueRegion:           *queueRegion,
		LambdaRegion:          *awsRegion,
		ReportingFrequency:    *reportingFrequency,
		RequestParameters:     requestParameters,
		StresstestTimeout:     *execTimeout,
		RunnerID:              *runnerID,
	}
	return lambdaSettings
}

// LambdaSettings represent the Lambdas configuration
type LambdaSettings struct {
	LambdaExecTimeoutSeconds int
	SqsURL                   string
	MaxRequestCount          int
	CompletedRequestCount    int
	StresstestTimeout        int
	ConcurrencyCount         int
	QueueRegion              string
	LambdaRegion             string
	ReportingFrequency       time.Duration
	ClientTimeout            time.Duration
	RequestParameters        requestParameters
	RunnerID                 int
}

// goadLambda holds the current state of the execution
type goadLambda struct {
	Settings      LambdaSettings
	HTTPClient    *http.Client
	Metrics       *requestMetric
	lambdaService lambdaiface.LambdaAPI
	resultSender  resultSender
	results       chan requestResult
	jobs          chan struct{}
	StartTime     time.Time
	wg            sync.WaitGroup
}

type requestParameters struct {
	URL            string
	ContentServer  string
	Requestcount   int
	RequestMethod  string
	RequestBody    string
	RequestHeaders []string
}

type requestResult struct {
	Time             int64  `json:"time"`
	Host             string `json:"host"`
	Type             string `json:"type"`
	Status           int    `json:"status"`
	ElapsedFirstByte int64  `json:"elapsed-first-byte"`
	ElapsedLastByte  int64  `json:"elapsed-last-byte"`
	Elapsed          int64  `json:"elapsed"`
	Bytes            int    `json:"bytes"`
	Timeout          bool   `json:"timeout"`
	ConnectionError  bool   `json:"connection-error"`
	State            string `json:"state"`
}

func (l *goadLambda) runLoadTest() {
	fmt.Printf("Using a timeout of %s\n", l.Settings.ClientTimeout)
	fmt.Printf("Using a reporting frequency of %s\n", l.Settings.ReportingFrequency)
	fmt.Printf("Will spawn %d workers making %d requests to %s\n", l.Settings.ConcurrencyCount, l.Settings.MaxRequestCount, l.Settings.RequestParameters.URL)

	l.StartTime = time.Now()

	l.spawnConcurrentWorkers()

	ticker := time.NewTicker(l.Settings.ReportingFrequency)
	quit := time.NewTimer(time.Duration(l.Settings.LambdaExecTimeoutSeconds) * time.Second)
	timedOut := false
	finished := false

	for !timedOut && !finished {
		select {
		case r := <-l.results:
			l.Settings.CompletedRequestCount++

			l.Metrics.addRequest(&r)
			if l.Settings.CompletedRequestCount%1000 == 0 || l.Settings.CompletedRequestCount == l.Settings.MaxRequestCount {
				fmt.Printf("\r%.2f%% done (%d requests out of %d)", (float64(l.Settings.CompletedRequestCount)/float64(l.Settings.MaxRequestCount))*100.0, l.Settings.CompletedRequestCount, l.Settings.MaxRequestCount)
			}
			continue

		case <-ticker.C:
			if l.Metrics.requestCountSinceLastSend > 0 {
				l.Metrics.sendAggregatedResults(l.resultSender)
				fmt.Printf("\nYay🎈  - %d requests completed\n", l.Settings.CompletedRequestCount)
			}
			continue

		case <-func() chan bool {
			quit := make(chan bool)
			go func() {
				l.wg.Wait()
				quit <- true
			}()
			return quit
		}():
			finished = true
			continue

		case <-quit.C:
			ticker.Stop()
			timedOut = true
			finished = l.updateStresstestTimeout()
		}
	}
	if timedOut && !finished {
		l.forkNewLambda()
	}
	l.Metrics.aggregatedResults.Finished = finished
	l.Metrics.sendAggregatedResults(l.resultSender)
	fmt.Printf("\nYay🎈  - %d requests completed\n", l.Settings.CompletedRequestCount)
}

// newLambda creates a new Lambda to execute a load test from a given
// LambdaSettings
func newLambda(s LambdaSettings) *goadLambda {
	setLambdaExecTimeout(&s)
	setDefaultConcurrencyCount(&s)

	l := &goadLambda{}
	l.Settings = s

	l.Metrics = NewRequestMetric(s.LambdaRegion, s.RunnerID)
	remainingRequestCount := s.MaxRequestCount - s.CompletedRequestCount
	if remainingRequestCount < 0 {
		remainingRequestCount = 0
	}
	l.setupHTTPClientForSelfsignedTLS()
	awsSqsConfig := l.setupAwsConfig()
	l.setupAwsSqsAdapter(awsSqsConfig)
	l.setupJobQueue(remainingRequestCount)
	l.results = make(chan requestResult)
	return l
}

func setDefaultConcurrencyCount(s *LambdaSettings) {
	if s.ConcurrencyCount < 1 {
		s.ConcurrencyCount = 1
	}
}

func setLambdaExecTimeout(s *LambdaSettings) {
	if s.StresstestTimeout <= 0 || s.StresstestTimeout > AWS_MAX_TIMEOUT {
		s.LambdaExecTimeoutSeconds = AWS_MAX_TIMEOUT
	} else {
		s.LambdaExecTimeoutSeconds = s.StresstestTimeout
	}
}

func (l *goadLambda) setupHTTPClientForSelfsignedTLS() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	l.HTTPClient = &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	l.HTTPClient.Timeout = l.Settings.ClientTimeout
}

func (l *goadLambda) setupAwsConfig() *aws.Config {
	return aws.NewConfig().WithRegion(l.Settings.QueueRegion)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func (l *goadLambda) setupAwsSqsAdapter(config *aws.Config) {
	rabbit := os.ExpandEnv("$RABBITMQ")
	if rabbit != "" {
		l.resultSender = newRabbitMQAdapter(rabbit)
	} else {
		l.resultSender = sqs.NewSQSAdapter(config, l.Settings.SqsURL)
	}
}

// RabbitMQAdapter to connect to RabbitMQ on docker daemon
type rabbitMQAdapter struct {
	resultSender
	ch   *amqp.Channel
	q    amqp.Queue
	conn *amqp.Connection
}

func (r *rabbitMQAdapter) SendResult(data api.RunnerResult) error {
	body, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = r.ch.Publish(
		"",       // exchange
		r.q.Name, // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	log.Printf(" [x] Sent %s", body)
	return err
}

func newRabbitMQAdapter(queueURL string) resultSender {
	conn, err := amqp.Dial(queueURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare(
		"goad", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return &rabbitMQAdapter{
		ch:   ch,
		q:    q,
		conn: conn,
	}
}

func (l *goadLambda) setupJobQueue(count int) {
	l.jobs = make(chan struct{}, count)
	for i := 0; i < count; i++ {
		l.jobs <- struct{}{}
	}
	close(l.jobs)
}

func (l *goadLambda) updateStresstestTimeout() bool {
	if l.Settings.StresstestTimeout != 0 {
		l.Settings.StresstestTimeout -= l.Settings.LambdaExecTimeoutSeconds
		return l.Settings.StresstestTimeout <= 0
	}
	return false
}

func (l *goadLambda) spawnConcurrentWorkers() {
	fmt.Print("Spawning workers…")
	for i := 0; i < l.Settings.ConcurrencyCount; i++ {
		l.spawnWorker()
		fmt.Print(".")
	}
	fmt.Println(" done.\nWaiting for results…")
}

func (l *goadLambda) spawnWorker() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		work(l)
	}()
}

func work(l *goadLambda) {
	for {
		if l.Settings.MaxRequestCount > 0 {
			_, ok := <-l.jobs
			if !ok {
				break
			}
		}
		l.results <- fetch(l.HTTPClient, l.Settings.RequestParameters, l.StartTime)
	}
}

func fetch(client *http.Client, p requestParameters, loadTestStartTime time.Time) requestResult {
	start := time.Now()
	req := prepareHttpRequest(p)
	response, err := client.Do(req)

	var status string
	var elapsedFirstByte time.Duration
	var elapsedLastByte time.Duration
	var elapsed time.Duration
	var statusCode int
	var bytesRead int
	buf := []byte(" ")
	timedOut := false
	connectionError := false
	isRedirect := err != nil && strings.Contains(err.Error(), "redirect")
	if err != nil && !isRedirect {
		status = fmt.Sprintf("ERROR: %s\n", err)
		switch err := err.(type) {
		case *url.Error:
			if err, ok := err.Err.(net.Error); ok && err.Timeout() {
				timedOut = true
			}
		case net.Error:
			if err.Timeout() {
				timedOut = true
			}
		}

		if !timedOut {
			connectionError = true
		}
	} else {
		statusCode = response.StatusCode
		elapsedFirstByte = time.Since(start)
		if !isRedirect {
			_, err = response.Body.Read(buf)
			firstByteRead := true
			if err != nil {
				status = fmt.Sprintf("reading first byte failed: %s\n", err)
				firstByteRead = false
			}
			body, err := ioutil.ReadAll(response.Body)
			if firstByteRead {
				bytesRead = len(body) + 1
			}
			elapsedLastByte = time.Since(start)
			if err != nil {
				// todo: detect timeout here as well
				status = fmt.Sprintf("reading response body failed: %s\n", err)
				connectionError = true
			} else {
				status = "Success"
			}
		} else {
			status = "Redirect"
		}
		response.Body.Close()

		elapsed = time.Since(start)
	}

	result := requestResult{
		Time:             start.Sub(loadTestStartTime).Nanoseconds(),
		Host:             req.URL.Host,
		Type:             req.Method,
		Status:           statusCode,
		ElapsedFirstByte: elapsedFirstByte.Nanoseconds(),
		ElapsedLastByte:  elapsedLastByte.Nanoseconds(),
		Elapsed:          elapsed.Nanoseconds(),
		Bytes:            bytesRead,
		Timeout:          timedOut,
		ConnectionError:  connectionError,
		State:            status,
	}
	return result
}

var letters = []byte ("abcdefghijklmnopqrstuvwyxz0123456789")
var events = []string {"error", "impression", "creativeView", "start", "firstQuartile", "Midpoint", "thirdQuartile", "complete", "custom_1", "custom_2", "custom_3", "custom_4", "custom_5", "custom_6", "custom_7", "click"}
var assetIds = []string {"p22yb33h7mqdpgk5fhm", "6veoeikmyj21a0w8fh7", "vy1na2pjfvw15bbawx3", "f4dv4hwv72uymum8fce", "2hdk0vudse6oga8ow36", "qne4eqygabb228yabba", "ntj6shdsjna4m3xbota", "vekgynmriba5wd27wyi", "2wndmb8zfjkfe2v0avk", "m5abucqz6cg55ysey33", "7svjw30b4wacc20dmof", "jdodlfmwbaypzs6vxry", "4dvr4n45313d6n8ojts", "tf2f4ihawwgstn55efp", "rt2hf16ykwt8rlvyykh", "olllvn0vs84h82byng2", "7bzqichddv8nq24yanr", "h3xwikfs35werhlm2k4", "haxeiuatyj1h304bpdj", "i6t8qbybw4qyb64ie1s"}

var longQueryString string = "p22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mqdpgk5fhm6veoeikmyj21a0w8fh7vy1na2pjfvw15bbawx3f4dv4hwv72uymum8fce2hdk0vudse6oga8ow36qne4eqygabb228yabbap22yb33h7mq"

var requestsIds = []string {"fvil-3pxoe","sotu-ivoa5","2pnz-fxmz3","g8ps-np67m","uh4w-wqqiw","l1lj-nuk7d","3edc-wcman","txs6-84h3j","j4s4-ixsje","fxa0-jys8u","cnc1-udgpz","bp0y-3z4c3","ibeq-2f3z8","z7b5-0o8ls","460f-p15lp","twco-psx0a","utwu-xrgzp","8wl8-6xhzx","0yan-typsc","p8wr-x1z2s","llzq-kiplt","kctk-xsk5l","gjuu-j5s8s","lfs5-l54pj","vvwc-p6n5r","6ihq-1eb4i","sum1-8sunn","mp05-if2ut","p2xu-zkyvt","70qn-hf1t6","jltj-fa5y7","mm7w-v4xll","uxc6-bw38e","p15a-ildpu","rduf-wj716","y8ir-d50yn","5w0e-a0kf4","apck-tvn4z","nsmu-sq5qz","dr67-nc7bl","m3su-074ev","c0j4-b411i","8h12-campj","0qj6-p1t44","jjdw-7t1zj","orgb-lyi0j","isck-50pwy","tb1s-pxcul","10kq-in1bn","agvi-v2upf","ofcx-a2kxn","ryip-7hndb","i63z-xb36a","0syc-748fg","ph6w-5hm1l","tsid-wszhb","cfot-2wdev","bvpq-7sbbw","klid-wjr6b","hrfp-wblxs","c22a-wqp8n","5d21-ab63k","08ux-czhgo","eq41-qjfdn","2otw-23tki","d0r0-p6vxt","fpth-viz53","zhm5-ub441","2tqs-c1rqb","s4cl-lzydu","woch-arnkx","wn6o-x20y4","vjrv-t5fxn","jzt4-l0m31","vu5y-f2gis","y7db-1o5hj","kwsm-adkwm","prwq-eg6hj","see4-edhih","r5p4-oazcv","dkho-ia0ju","xyxb-4stp8","f8mb-rrfer","zve1-quc5t","2x55-gtliq","b8cu-kidnc","0auf-u0gkl","hg06-7es03","k3l4-vyy7k","auk4-ypahl","hisv-f7ssf","x33s-copor","gugj-j3i53","yikx-l1quj","ba0h-dmzbp","cd38-tkpa8","jzrb-0waa2","5bnw-3xq4t","5e11-i32zn","mfng-p1zve","ljt1-33ebc","b0ei-iml4q","mvws-j2gba","bpcy-7gv1d","qfjr-6ycqm","e7nz-l80zj","zqzb-cnv8r","muo0-h3ab7","ipzf-g4eaq","pnl0-5aatp","zpnm-cxbf2","t21u-k7qs3","dizo-04h4f","3xua-3wg0q","fyt2-v6ybl","mwcz-ef503","rfde-n0efm","miex-vj8wr","mcsb-mmqn7","hyob-3iz0e","coui-acwtj","04k4-wnpxw","80vs-ec4xr","d7cz-wpq4u","eaza-nx8tk","jd2x-0riwk","xrjy-kahhk","byso-dgp3l","jwsk-n5ub4","38uw-6z1qg","p2l0-nm88w","nts0-u4ehf","goe7-cobbu","qacu-74tsf","4rak-h243i","qxui-1jhgc","sh01-l7q0z","xa7j-i2go4","41u6-cqo6u","7erk-86v4t","5zee-zqijm","e4yy-pn2kq","jbpv-iy4ea","7end-at5sy","kcds-en2pn","hltw-gs38w","mkdb-am6vr","e51k-lo6nr","m00j-dtzzi","a6og-3o04c","t6i5-fhmes","h7ry-yme45","mn5q-f76zm","d24f-xoy2n","hoi1-ij1y8","qpop-oyn8e","ilva-437gb","8hrz-5d7zy","0z3o-lxeik","l3vx-gfgsl","8bow-q5npy","eqi8-7t4el","r7hv-qu1b7","yq7q-veolv","hsrb-zny4w","5wqa-ubb5s","o8e5-u5rso","i2y3-xh3ma","a5i7-2mowx","1j11-qqkst","0o7j-06szp","1ifi-yf6kk","rn68-gol7l","35ci-4ggn8","jr8f-1x6i6","wp1g-5lxyx","cqw2-c2zej","jvot-whxb2","808f-w6cbg","bh0h-m8iug","8okz-dk3rn","mlky-dgt2k","ntky-0t4ic","o8p7-ynvg1","1jt5-dr5k4","b8w7-1qlpa","zq4g-sko6y","ti1d-k6kqp","mxta-2zctx","3bm8-3zkbw","iob3-fnxtt","g55c-6jx7k","tq5f-tl1sy","wvv8-s1djo","osfs-6gtfr","gyrh-6d4h1","j36j-gdqug","iyh0-vqrp7","smla-vequj","v15k-1si1s"}


func randomRequestId() string {
	return requestsIds[rand.Intn(200)] + "-" + requestsIds[rand.Intn(200)]
}


func randomEvent() string {
	return events[rand.Intn(16)]
}

func randomAssetCreativeId() string {
	return "/my_first_creative/" + assetIds[rand.Intn(20)]
}


func buildLink(url string) string {
	var requestUrl bytes.Buffer

	var event = randomEvent()
	var path string = event + "/dcn" + randomAssetCreativeId() // 1
	var queryStrings bytes.Buffer

	queryStrings.WriteString("?")
	queryStrings.WriteString("request_id=" + randomRequestId()) // 3
	queryStrings.WriteString("&device_id=" + randomRequestId()) // 5
	queryStrings.WriteString("&duration=25")
	queryStrings.WriteString("&cb=" + strconv.Itoa(543128412389 + rand.Intn(1432894012))) // 6
	queryStrings.WriteString("&long_query=" + longQueryString + strconv.Itoa(rand.Intn(10000000000000))) // 7

	if event == "click" {
		queryStrings.WriteString("&click_through=https%3A%2F%2Fs3.amazonaws.com%2Fstresstestbucket%2Fok")
	}

	requestUrl.WriteString(url)
	requestUrl.WriteString("/")
	requestUrl.WriteString(path)
	requestUrl.WriteString("/")
	requestUrl.WriteString(queryStrings.String())

	return requestUrl.String()
}



func prepareHttpRequest(params requestParameters) *http.Request {
	var URL = buildLink(params.URL)
	if rand.Intn(17) == 0 {
		URL = params.ContentServer + "&long_query=" + longQueryString + strconv.Itoa(rand.Intn(10000000000000))
	}
	fmt.Println("Pinging:", URL)

	req, err := http.NewRequest(params.RequestMethod, URL, bytes.NewBufferString(params.RequestBody))
	if err != nil {
		fmt.Println("Error creating the HTTP request:", err)
		panic("")
	}
	req.Header.Add("Accept-Encoding", "gzip")
	for _, v := range params.RequestHeaders {
		header := strings.Split(v, ":")
		if strings.ToLower(strings.Trim(header[0], " ")) == "host" {
			req.Host = strings.Trim(header[1], " ")
		} else {
			req.Header.Add(strings.Trim(header[0], " "), strings.Trim(header[1], " "))
		}
	}

	if req.Header.Get("User-Agent") == "" {
		req.Header.Add("User-Agent", "Mozilla/5.0 (compatible; Goad/1.0; +https://goad.io)")
	}
	return req
}


type requestMetric struct {
	aggregatedResults         *api.RunnerResult
	firstRequestTime          int64
	lastRequestTime           int64
	timeToFirstTotal          int64
	requestTimeTotal          int64
	requestCountSinceLastSend int64
}

type resultSender interface {
	SendResult(api.RunnerResult) error
}

func NewRequestMetric(region string, runnerID int) *requestMetric {
	metric := &requestMetric{
		aggregatedResults: &api.RunnerResult{
			Region:   region,
			RunnerID: runnerID,
		},
	}
	metric.resetAndKeepTotalReqs()
	return metric
}

func (m *requestMetric) addRequest(r *requestResult) {
	agg := m.aggregatedResults
	agg.RequestCount++
	m.requestCountSinceLastSend++
	if m.firstRequestTime == 0 {
		m.firstRequestTime = r.Time
	}
	m.lastRequestTime = r.Time + r.Elapsed

	if r.Timeout {
		agg.TimedOut++
	} else if r.ConnectionError {
		agg.ConnectionErrors++
	} else {
		agg.BytesRead += r.Bytes
		m.requestTimeTotal += r.ElapsedLastByte
		m.timeToFirstTotal += r.ElapsedFirstByte

		agg.Fastest = Min(r.ElapsedLastByte, agg.Fastest)
		agg.Slowest = Max(r.ElapsedLastByte, agg.Slowest)

		statusStr := strconv.Itoa(r.Status)
		_, ok := agg.Statuses[statusStr]
		if !ok {
			agg.Statuses[statusStr] = 1
		} else {
			agg.Statuses[statusStr]++
		}
	}
	m.aggregate()
}

func (m *requestMetric) aggregate() {
	agg := m.aggregatedResults
	countOk := int(m.requestCountSinceLastSend) - (agg.TimedOut + agg.ConnectionErrors)
	agg.TimeDelta = time.Duration(m.lastRequestTime-m.firstRequestTime) * time.Nanosecond
	if countOk > 0 {
		agg.AveTimeToFirst = m.timeToFirstTotal / int64(countOk)
		agg.AveTimeForReq = m.requestTimeTotal / int64(countOk)
	}
	agg.FatalError = ""
	if (agg.TimedOut + agg.ConnectionErrors) > int(m.requestCountSinceLastSend)/2 {
		agg.FatalError = "Over 50% of requests failed, aborting"
	}
}

func (m *requestMetric) sendAggregatedResults(sender resultSender) {
	err := sender.SendResult(*m.aggregatedResults)
	failOnError(err, "Failed to send data to cli")
	m.resetAndKeepTotalReqs()
}

func (m *requestMetric) resetAndKeepTotalReqs() {
	m.requestCountSinceLastSend = 0
	m.firstRequestTime = 0
	m.lastRequestTime = 0
	m.requestTimeTotal = 0
	m.timeToFirstTotal = 0
	m.aggregatedResults = &api.RunnerResult{
		Region:   m.aggregatedResults.Region,
		RunnerID: m.aggregatedResults.RunnerID,
		Statuses: make(map[string]int),
		Fastest:  math.MaxInt64,
		Finished: false,
	}
}

func (l *goadLambda) forkNewLambda() {
	svc := l.provideLambdaService()
	args := l.getInvokeArgsForFork()

	j, _ := json.Marshal(args)

	output, err := svc.InvokeAsync(&lambda.InvokeAsyncInput{
		FunctionName: aws.String("goad"),
		InvokeArgs:   bytes.NewReader(j),
	})
	fmt.Println(output)
	fmt.Println(err)
}

func (l *goadLambda) provideLambdaService() lambdaiface.LambdaAPI {
	if l.lambdaService == nil {
		l.lambdaService = lambda.New(session.New(), aws.NewConfig().WithRegion(l.Settings.LambdaRegion))
	}
	return l.lambdaService
}

func (l *goadLambda) getInvokeArgsForFork() invokeArgs {
	args := newLambdaInvokeArgs()
	settings := l.Settings
	params := settings.RequestParameters
	args.Flags = []string{
		fmt.Sprintf("--concurrency=%s", strconv.Itoa(settings.ConcurrencyCount)),
		fmt.Sprintf("--requests=%s", strconv.Itoa(settings.MaxRequestCount)),
		fmt.Sprintf("--completed-count=%s", strconv.Itoa(l.Settings.CompletedRequestCount)),
		fmt.Sprintf("--execution-time=%s", strconv.Itoa(settings.StresstestTimeout)),
		fmt.Sprintf("--sqsurl=%s", settings.SqsURL),
		fmt.Sprintf("--queue-region=%s", settings.QueueRegion),
		fmt.Sprintf("--client-timeout=%s", settings.ClientTimeout),
		fmt.Sprintf("--runner-id=%d", settings.RunnerID),
		fmt.Sprintf("--frequency=%s", settings.ReportingFrequency),
		fmt.Sprintf("--aws-region=%s", settings.LambdaRegion),
		fmt.Sprintf("--method=%s", settings.RequestParameters.RequestMethod),
		fmt.Sprintf("--body=%s", settings.RequestParameters.RequestBody),
	}
	args.Flags = append(args.Flags, fmt.Sprintf("%s", params.URL))
	args.Flags = append(args.Flags, fmt.Sprintf("%s", params.ContentServer))
	fmt.Println(args.Flags)
	return args
}

type invokeArgs struct {
	File  string   `json:"file"`
	Flags []string `json:"args"`
}

func newLambdaInvokeArgs() invokeArgs {
	return invokeArgs{
		File: "./goad-lambda",
	}
}

// Min calculates minimum of two int64
func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// Max calculates maximum of two int64
func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
