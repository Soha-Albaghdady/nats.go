// Copyright 2015-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
	DefaultNumRuns     = 1
)

func usage() {
	log.Printf("Usage: nats-bench [-s server (%s)] [--tls] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] [-ms MESSAGE_SIZE] [-csv csvfile] [-nr NUM_RUNS] <subject>\n", nats.DefaultURL)
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

var benchmark *bench.Benchmark

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var tls = flag.Bool("tls", false, "Use TLS Secure Connection")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var csvFile = flag.String("csv", "", "Save bench data to csv file")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showHelp = flag.Bool("h", false, "Show help message")
	var numRuns = flag.Int("nr", DefaultNumRuns, "Number of Benchmark Runs")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	if *numMsgs <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Benchmark")}

  // Use UserCredentials
  if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
 }

	// Use TLS specified
	if *tls {
		opts = append(opts, nats.Secure(nil))
	}
	for i := 0; i < *numRuns; i++ {
		fmt.Printf("Starting Benchmark Run ..")
		benchmark = bench.NewBenchmark("NATS", *numSubs, *numPubs)

		var startwg sync.WaitGroup
		var donewg sync.WaitGroup

		donewg.Add(*numPubs + *numSubs)

		// Run Subscribers first
		startwg.Add(*numSubs)
		for i := 0; i < *numSubs; i++ {
			nc, err := nats.Connect(*urls, opts...)
			if err != nil {
					log.Fatalf("Can't connect: %v\n", err)
			}
			defer nc.Close()

			go runSubscriber(nc, &startwg, &donewg, *numMsgs, *msgSize)
			}
			startwg.Wait()

			// Now Publishers
			startwg.Add(*numPubs)
			pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)
			for i := 0; i < *numPubs; i++ {
				nc, err := nats.Connect(*urls, opts...)
				if err != nil {
						log.Fatalf("Can't connect: %v\n", err)
				}
				defer nc.Close()

				go runPublisher(nc, &startwg, &donewg, pubCounts[i], *msgSize)
			}

			log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d, runs=%d]\n", *numMsgs, *msgSize, *numPubs, *numSubs, *numRuns)

			startwg.Wait()
			donewg.Wait()

			benchmark.Close()

			fmt.Print(benchmark.Report())

			if len(*csvFile) > 0 {
				csv := benchmark.CSV()
				ioutil.WriteFile(*csvFile, []byte(csv), 0644)
				fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
			}
			time.Sleep(20 * time.Second)
			log.Printf("Run %d",i)
	}

	}

func runPublisher(nc *nats.Conn, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int) {
		  startwg.Done()

		  args := flag.Args()
		  subj := args[0]

		  start := time.Now()

		  for i := 0; i < numMsgs; i++ {
		          msg:="{\"host\":\"10.0.139.9\",\"port\":61063,\"uris\":[\"test-nats" + strconv.Itoa(i) + "-app.cf.cfi05.aws.cfi.sapcloud.io\"],\"app\":\"29449583-ba01-4982-a915-fd1597b5dd6a\",\"private_instance_id\":\"3213c9e4-8cbc-435a-6f83-3faa\",\"private_instance_index\":\"0\",\"server_cert_domain_san\":\"3213c9e4-8cbc-435a-6f83-3faa\",\"tags\":{\"app_id\":\"29449583-ba01-4982-a915-fd1597b5dd6a\",\"app_name\":\"zfse-00052384-approuter\",\"component\":\"route-emitter\",\"instance_id\":\"0\",\"organization_id\":\"4b705754-2fe8-4055-873c-b2915ab0124d\",\"organization_name\":\"d12_daf\",\"process_id\":\"29449583-ba01-4982-a915-fd1597b5dd6a\",\"process_instance_id\":\"3213c9e4-8cbc-435a-6f83-3faa\",\"process_type\":\"web\",\"source_id\":\"29449583-ba01-4982-a915-fd1597b5dd6a\",\"space_id\":\"d1d003b5-4256-4991-8e8a-15a43355941e\",\"space_name\":\"SharedServices\"}}"
		  nc.Publish(subj, []byte(msg))
		  }
		  nc.Flush()
		  benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), nc))

		  donewg.Done()
}

func runSubscriber(nc *nats.Conn, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int) {
		  args := flag.Args()
		  subj := args[0]

		  received := 0
		  ch := make(chan time.Time, 2)
		  sub, _ := nc.Subscribe(subj, func(msg *nats.Msg) {
		          received++
		          if received == 1 {
		                  ch <- time.Now()
		          }
		          if received >= numMsgs {
		                  ch <- time.Now()
		          }
		  })
		  sub.SetPendingLimits(-1, -1)
		  nc.Flush()
		  startwg.Done()

		  start := <-ch
		  end := <-ch
		  benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, end, nc))
		  nc.Close()
		  donewg.Done()
}
