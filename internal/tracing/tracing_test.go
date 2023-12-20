package tracing

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

const (
	testTraceID     = "0123456789abcdef0123456789abcdef"
	testCallerID    = "fedcba9876543210"
	testTraceParent = traceVersion + "-" + testTraceID + "-" + testCallerID + "-" + traceFlags

	testCallerName  = "test"
	testTraceState  = testCallerName + "=" + testCallerID
)

func TestNoGenerate(t *testing.T) {
	req := makeReq(t)
	headers := GetTraceHeaders(req, false)
	require.Equal(t, len(headers), 0)
}

func TestOnlyParentHeader(t *testing.T) {
	req := makeReq(t)
	req.Header.Add(traceParentHeader, testTraceParent)

	headers := GetTraceHeaders(req, false)
	require.Equal(t, len(headers), 2)
	validateTraceParentHeader(t, headers)
	for _, h := range headers {
		if string(h.Key) == traceStateHeader {
			require.Equal(t, string(h.Value), fmt.Sprintf("%s=%s", traceAppName, traceAppID))
		}
	}
}

func TestOnlyStateHeader(t *testing.T) {
	req := makeReq(t)
	req.Header.Add(traceStateHeader, testTraceState)
	headers := GetTraceHeaders(req, false)
	require.Equal(t, len(headers), 0)
}

func TestParentPlusStateHeaders(t *testing.T) {
	req := makeReq(t)
	req.Header.Add(traceParentHeader, testTraceParent)
	req.Header.Add(traceStateHeader, testTraceState)
	headers := GetTraceHeaders(req, false)
	require.Equal(t, len(headers), 2)
	validateTraceParentHeader(t, headers)
	for _, h := range headers {
		if string(h.Key) == traceStateHeader {
			require.Equal(t, string(h.Value), fmt.Sprintf("%s=%s,%s", traceAppName, traceAppID, testTraceState))
		}
	}
}

func TestTraceHeaderGeneration(t *testing.T) {
	testTraceHeaderGeneration(t)
}

func BenchmarkIndependentRandGen(b *testing.B) {
	url := "/testurl"
	var body []byte
	req, _ := http.NewRequest("GET", url, bytes.NewReader(body))
	var wg sync.WaitGroup

	for n := 0; n < b.N; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			GetTraceHeaders(req, true)
		}()
	}
	wg.Wait()
}

func validateTraceParentHeader(t *testing.T, headers []sarama.RecordHeader) {
	for _, h := range headers {
		key := string(h.Key)
		val := string(h.Value)
		if key == traceParentHeader {
			traceComponents := strings.Split(val, "-")
			require.Equal(t, len(traceComponents), 4)
			require.Equal(t, traceComponents[0], traceVersion)
			require.Equal(t, traceComponents[1], testTraceID)
			require.Equal(t, traceComponents[2], traceAppID)
			require.Equal(t, traceComponents[3], traceFlags)
		}
	}
}

func makeReq(t *testing.T) *http.Request {
	url := "/testurl"
	var body []byte
	req, err := http.NewRequest("GET", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func testTraceHeaderGeneration(t *testing.T) {
	req := makeReq(t)
	headers := GetTraceHeaders(req, true)
	require.Equal(t, len(headers), 2)
	for _, h := range headers {
		key := string(h.Key)
		val := string(h.Value)
		if key == traceParentHeader {
			traceComponents := strings.Split(val, "-")
			require.Equal(t, len(traceComponents), 4)
			require.Equal(t, traceComponents[0], traceVersion)
			// We don't know the ID here, we can only check it is 32 digits long
			require.Equal(t, len(traceComponents[1]), 32)
			require.Equal(t, traceComponents[2], traceAppID)
			require.Equal(t, traceComponents[3], traceFlags)
		}
		if key == traceStateHeader {
			require.Equal(t, val, fmt.Sprintf("%s=%s", traceAppName, traceAppID))
		}
	}
}


func TestSetTracingConfig(t *testing.T) {
	cases := []struct {
		appName         string
		appId           string
		expectedAppName string
		expectedAppId   string
	}{
		{
			expectedAppName: "hc",
			expectedAppId:   "0000000000000001",
		},
		{
			appName:         "dummy_app_name",
			appId:           "dummy_app_id",
			expectedAppName: "dummy_app_name",
			expectedAppId:   "dummy_app_id",
		},
	}

	for _, c := range cases {
		SetTracingConfig(c.appName, c.appId)
		require.Equal(t, c.expectedAppName, traceAppName)
		require.Equal(t, c.expectedAppId, traceAppID)
	}
}
