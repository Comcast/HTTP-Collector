package radix

import (
	"context"
)

// NewTopicContext returns a new routing Context object.
func NewTopicContext() *Context {
	return &Context{}
}

var (
	// RouteCtxKey is the context.Context key to store the request context.
	RouteCtxKey = &contextKey{"RouteContext"}
)

// Context is the default routing context set on the root node of a
// request context to track route patterns, URL parameters and
// an optional routing path.
type Context struct {
	// parentCtx is the parent of this one, for using Context as a
	// context.Context directly. This is an optimization that saves
	// 1 allocation.
	parentCtx context.Context

	// TopicParams are the stack of routeParams captured during the
	// routing lifecycle across a stack of sub-routers.
	TopicParams TopicParams

	// Route parameters matched for the current sub-router. It is
	// intentionally unexported so it cant be tampered.
	topicParams TopicParams

	// The endpoint routing pattern that matched the request URI path
	// or `RoutePath` of the current sub-router. This value will update
	// during the lifecycle of a request passing through a stack of
	// sub-routers.
	topicPattern string

	// Routing pattern stack throughout the lifecycle of the request,
	// across all connected routers. It is a record of all matching
	// patterns across a stack of sub-routers.
	TopicPatterns []string
}

// Reset a routing context to its initial state.
func (x *Context) Reset() {
	x.topicPattern = ""
	x.TopicPatterns = x.TopicPatterns[:0]

	x.topicParams.Keys = x.topicParams.Keys[:0]
	x.topicParams.Values = x.topicParams.Values[:0]
	x.TopicParams.Keys = x.TopicParams.Keys[:0]
	x.TopicParams.Values = x.TopicParams.Values[:0]

	x.parentCtx = nil
}

// Param returns the corresponding parameter value from the request
// routing context.
func (x *Context) Param(key string) string {
	for k := len(x.TopicParams.Keys) - 1; k >= 0; k-- {
		if x.TopicParams.Keys[k] == key {
			return x.TopicParams.Values[k]
		}
	}
	return ""
}

// TopicParams is a structure to track URL routing parameters efficiently.
type TopicParams struct {
	Keys, Values []string
}

// Add will append a URL parameter to the end of the route param
func (s *TopicParams) Add(key, value string) {
	s.Keys = append(s.Keys, key)
	s.Values = append(s.Values, value)
}

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation. This technique
// for defining context keys was copied from Go 1.7's new use of context in net/http.
type contextKey struct {
	name string
}

func (k *contextKey) String() string {
	return "chi context value " + k.name
}
