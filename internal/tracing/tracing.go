/*
	W3C Doc: https://www.w3.org/TR/trace-context-1

	Spec:
	traceparent header has 4 sections
		"<version>-<traceID>-<callerID>-<Flags>"
		version is 2 digits, traceID is 32 digits, callerIF is 16, and flags is 2
	if traceparent is present in headers, replace the callerID with hc's ID

	hc's ID defaults to "0000000000000001" and can be changed in config

	If tracestate header exists, add "hc=<hcID>," to the beginning of the header
	else set it to "hc=<hcID>"

	As of now, we don't generate traceparent if the header is not passed in
	Any trace flag setting that tells us to regenerate the traceID is ignored.
	Version, TraceID, and trace flags are passed as-is. Only CallerID is modified
	TraceState will be generated if it doesn't exist

	Ticket: ODP-25026
*/

package tracing

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	traceParentHeader = "traceparent"
	traceStateHeader  = "tracestate"

	// These are two completely internal
	traceVersion = "01"
	traceFlags   = "00"
)

var (
	traceAppName = "hc"
	// 16 char arbitrary string ID identifying app for tracing
	traceAppID   = "0000000000000001"
)

func SetTracingConfig(appName string, appID string) {
	if appName != "" {
		traceAppName = appName
	}
	if appID != "" {
		traceAppID   = appID
	}
}

func GetTraceHeaders(r *http.Request, generateTrace bool) []sarama.RecordHeader {
	// This is not concurrency safe (hat tip to Jay)
	// Hence it is instantiated in the goroutine that handles the API req
	// Google search says this is inexpensive and acceptable
	// Prefer not to use a mutex as multiple Kafka reqs can wait on this lock

	var headers []sarama.RecordHeader
	traceParent := r.Header.Get(traceParentHeader)
	traceComponents := strings.Split(traceParent, "-")
	if len(traceComponents) != 4 {
		// traceparent header is either incorrectly formatted or doesn't exist
		if !generateTrace {
			// Don't create traceparent header if flag is not set
			// Don't bother about tracestate header
			return headers
		}

		// Create a new traceparent header
		randGen := rand.NewSource(time.Now().UnixNano())
		traceParent = fmt.Sprintf("%s-%016x%016x-%s-%s",
			traceVersion,
			// genInt63(randGen), genInt63(randGen),
			randGen.Int63(), randGen.Int63(),
			traceAppID,
			traceFlags)
	} else {
		// Replace the appID part
		traceParent = fmt.Sprintf("%s-%s-%s-%s",
			traceComponents[0],
			traceComponents[1],
		 	traceAppID,
			traceComponents[3])
	}
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("traceparent"),
		Value: []byte(traceParent),
	})

	traceState := r.Header.Get(traceStateHeader)
	if traceState == "" {
		// Create tracestate header if it doesn't exist
		traceState = fmt.Sprintf("%s=%s", traceAppName, traceAppID)
	} else {
		// Add current app to traceState
		traceState = fmt.Sprintf("%s=%s,%s", traceAppName, traceAppID, traceState)
	}
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("tracestate"),
		Value: []byte(traceState),
	})
	return headers
}
