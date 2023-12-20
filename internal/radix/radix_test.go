package radix

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRadix(t *testing.T) {
	hStub1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub4 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub5 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub6 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub7 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	// TODO: panic if we see {id}{x} because we're missing a delimiter, its not possible.
	// also {:id}* is not possible.

	tr := &Node[http.Handler]{}

	tr.InsertTopic("a/*", hStub1)
	tr.InsertTopic("a/{x}", hStub2)
	tr.InsertTopic("a/{x}/*", hStub3)
	tr.InsertTopic("a/{x}/{y}", hStub4)
	tr.InsertTopic("b/*", hStub5)
	tr.InsertTopic("b/x", hStub6)
	tr.InsertTopic("b/y", hStub7)

	tests := []struct {
		h http.Handler
		r string
		k []string
		v []string
	}{
		{r: "a/id1", h: hStub2, k: []string{"x"}, v: []string{"id1"}},
		{r: "a/id1/", h: hStub3, k: []string{"x", "*"}, v: []string{"id1", ""}},
		{r: "a/id1/id2", h: hStub4, k: []string{"x", "y"}, v: []string{"id1", "id2"}},
		{r: "a/id1/id2/something", h: hStub3, k: []string{"x", "*"}, v: []string{"id1", "id2/something"}},
		{r: "b/x", h: hStub6, k: nil, v: nil},
		{r: "b/y", h: hStub7, k: nil, v: nil},
		{r: "b/something", h: hStub5, k: []string{"*"}, v: []string{"something"}},
		{r: "b/something/", h: hStub5, k: []string{"*"}, v: []string{"something/"}},
		{r: "b/something/z", h: hStub5, k: []string{"*"}, v: []string{"something/z"}},
		{r: "c", h: nil, k: nil, v: nil},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, handlers, _ := tr.FindTopic(rctx, tt.r)

		var handler http.Handler
		if handlers != nil {
			handler = handlers.handler
		}

		paramKeys := rctx.topicParams.Keys
		paramValues := rctx.topicParams.Values

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
		require.Equal(t, tt.k, paramKeys, "index %d", i)
		require.Equal(t, tt.v, paramValues, "index %d", i)
	}
}

func TestTree(t *testing.T) {
	hStub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hIndex := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hFavicon := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleList := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleNear := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleShow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleShowRelated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleShowOpts := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleSlug := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hArticleByUser := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hUserList := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hUserShow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hAdminCatchall := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hAdminAppShow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hAdminAppShowCatchall := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hUserProfile := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hUserSuper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hUserAll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hHubView1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hHubView2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hHubView3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	tr := &Node[http.Handler]{}

	tr.InsertTopic("/", hIndex)
	tr.InsertTopic("/favicon.ico", hFavicon)

	tr.InsertTopic("/pages/*", hStub)

	tr.InsertTopic("/article", hArticleList)
	tr.InsertTopic("/article/", hArticleList)

	tr.InsertTopic("/article/near", hArticleNear)
	tr.InsertTopic("/article/{id}", hStub)
	tr.InsertTopic("/article/{id}", hArticleShow)
	tr.InsertTopic("/article/{id}", hArticleShow) // duplicate will have no effect
	tr.InsertTopic("/article/@{user}", hArticleByUser)

	tr.InsertTopic("/article/{sup}/{opts}", hArticleShowOpts)
	tr.InsertTopic("/article/{id}/{opts}", hArticleShowOpts) // overwrite above route, latest wins

	tr.InsertTopic("/article/{iffd}/edit", hStub)
	tr.InsertTopic("/article/{id}//related", hArticleShowRelated)
	tr.InsertTopic("/article/slug/{month}/-/{day}/{year}", hArticleSlug)

	tr.InsertTopic("/admin/user", hUserList)
	tr.InsertTopic("/admin/user/", hStub) // will get replaced by next route
	tr.InsertTopic("/admin/user/", hUserList)

	tr.InsertTopic("/admin/user//{id}", hUserShow)
	tr.InsertTopic("/admin/user/{id}", hUserShow)

	tr.InsertTopic("/admin/apps/{id}", hAdminAppShow)
	tr.InsertTopic("/admin/apps/{id}/*", hAdminAppShowCatchall)

	tr.InsertTopic("/admin/*", hStub) // catchall segment will get replaced by next route
	tr.InsertTopic("/admin/*", hAdminCatchall)

	tr.InsertTopic("/users/{userID}/profile", hUserProfile)
	tr.InsertTopic("/users/super/*", hUserSuper)
	tr.InsertTopic("/users/*", hUserAll)

	tr.InsertTopic("/hubs/{hubID}/view", hHubView1)
	tr.InsertTopic("/hubs/{hubID}/view/*", hHubView2)
	tr.InsertTopic("/hubs/{hubID}/users", hHubView3)

	tests := []struct {
		r string       // input request path
		h http.Handler // output matched handler
		k []string     // output param keys
		v []string     // output param values
	}{
		{r: "/", h: hIndex, k: nil, v: nil},
		{r: "/favicon.ico", h: hFavicon, k: nil, v: nil},

		{r: "/pages", h: nil, k: nil, v: nil},
		{r: "/pages/", h: hStub, k: []string{"*"}, v: []string{""}},
		{r: "/pages/yes", h: hStub, k: []string{"*"}, v: []string{"yes"}},

		{r: "/article", h: hArticleList, k: nil, v: nil},
		{r: "/article/", h: hArticleList, k: nil, v: nil},
		{r: "/article/near", h: hArticleNear, k: nil, v: nil},
		{r: "/article/neard", h: hArticleShow, k: []string{"id"}, v: []string{"neard"}},
		{r: "/article/123", h: hArticleShow, k: []string{"id"}, v: []string{"123"}},
		{r: "/article/123/456", h: hArticleShowOpts, k: []string{"id", "opts"}, v: []string{"123", "456"}},
		{r: "/article/@peter", h: hArticleByUser, k: []string{"user"}, v: []string{"peter"}},
		{r: "/article/22//related", h: hArticleShowRelated, k: []string{"id"}, v: []string{"22"}},
		{r: "/article/111/edit", h: hStub, k: []string{"iffd"}, v: []string{"111"}},
		{r: "/article/slug/sept/-/4/2015", h: hArticleSlug, k: []string{"month", "day", "year"}, v: []string{"sept", "4", "2015"}},
		{r: "/article/:id", h: hArticleShow, k: []string{"id"}, v: []string{":id"}},

		{r: "/admin/user", h: hUserList, k: nil, v: nil},
		{r: "/admin/user/", h: hUserList, k: nil, v: nil},
		{r: "/admin/user/1", h: hUserShow, k: []string{"id"}, v: []string{"1"}},
		{r: "/admin/user//1", h: hUserShow, k: []string{"id"}, v: []string{"1"}},
		{r: "/admin/hi", h: hAdminCatchall, k: []string{"*"}, v: []string{"hi"}},
		{r: "/admin/lots/of/:fun", h: hAdminCatchall, k: []string{"*"}, v: []string{"lots/of/:fun"}},
		{r: "/admin/apps/333", h: hAdminAppShow, k: []string{"id"}, v: []string{"333"}},
		{r: "/admin/apps/333/woot", h: hAdminAppShowCatchall, k: []string{"id", "*"}, v: []string{"333", "woot"}},

		{r: "/hubs/123/view", h: hHubView1, k: []string{"hubID"}, v: []string{"123"}},
		{r: "/hubs/123/view/index.html", h: hHubView2, k: []string{"hubID", "*"}, v: []string{"123", "index.html"}},
		{r: "/hubs/123/users", h: hHubView3, k: []string{"hubID"}, v: []string{"123"}},

		{r: "/users/123/profile", h: hUserProfile, k: []string{"userID"}, v: []string{"123"}},
		{r: "/users/super/123/okay/yes", h: hUserSuper, k: []string{"*"}, v: []string{"123/okay/yes"}},
		{r: "/users/123/okay/yes", h: hUserAll, k: []string{"*"}, v: []string{"123/okay/yes"}},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, handlers, _ := tr.FindTopic(rctx, tt.r)

		var handler http.Handler
		if handlers != nil {
			handler = handlers.handler
		}

		paramKeys := rctx.topicParams.Keys
		paramValues := rctx.topicParams.Values

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
		require.Equal(t, tt.k, paramKeys, "index %d", i)
		require.Equal(t, tt.v, paramValues, "index %d", i)
	}
}

func TestTreeMoar(t *testing.T) {
	hStub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub4 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub5 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub6 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub7 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub8 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub9 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub10 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub11 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub12 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub13 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub14 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub15 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub16 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	// TODO: panic if we see {id}{x} because we're missing a delimiter, its not possible.
	// also {:id}* is not possible.

	tr := &Node[http.Handler]{}

	tr.InsertTopic("/articlefun", hStub5)
	tr.InsertTopic("/articles/{id}", hStub)
	tr.InsertTopic("/articles/search", hStub1)
	tr.InsertTopic("/articles/{id}:delete", hStub8)
	tr.InsertTopic("/articles/{iidd}!sup", hStub4)
	tr.InsertTopic("/articles/{id}:{op}", hStub3)
	tr.InsertTopic("/articles/{id}:{op}", hStub2) // this route sets a new handler for the above route
	tr.InsertTopic("/articles/{slug:^[a-z]+}/posts", hStub) // up to tail '/' will only match if contents match the rex
	tr.InsertTopic("/articles/{id}/posts/{pid}", hStub6) // /articles/123/posts/1
	tr.InsertTopic("/articles/{id}/posts/{month}/{day}/{year}/{slug}", hStub7) // /articles/123/posts/09/04/1984/juice
	tr.InsertTopic("/articles/{id}.json", hStub10)
	tr.InsertTopic("/articles/{id}/data.json", hStub11)
	tr.InsertTopic("/articles/files/{file}.{ext}", hStub12)
	tr.InsertTopic("/articles/me", hStub13)

	// TODO: make a separate test case for this one..
	// tr.InsertRoute(mGET, "/articles/{id}/{id}", hStub1) // panic expected, we're duplicating param keys

	tr.InsertTopic("/pages/*", hStub)
	tr.InsertTopic("/pages/*", hStub9)

	tr.InsertTopic("/users/{id}", hStub14)
	tr.InsertTopic("/users/{id}/settings/{key}", hStub15)
	tr.InsertTopic("/users/{id}/settings/*", hStub16)

	tests := []struct {
		h http.Handler
		r string
		k []string
		v []string
	}{
		{r: "/articles/search", h: hStub1, k: nil, v: nil},
		{r: "/articlefun", h: hStub5, k: nil, v: nil},
		{r: "/articles/123", h: hStub, k: []string{"id"}, v: []string{"123"}},
		{r: "/articles/789:delete", h: hStub8, k: []string{"id"}, v: []string{"789"}},
		{r: "/articles/789!sup", h: hStub4, k: []string{"iidd"}, v: []string{"789"}},
		{r: "/articles/123:sync", h: hStub2, k: []string{"id", "op"}, v: []string{"123", "sync"}},
		{r: "/articles/456/posts/1", h: hStub6, k: []string{"id", "pid"}, v: []string{"456", "1"}},
		{r: "/articles/456/posts/09/04/1984/juice", h: hStub7, k: []string{"id", "month", "day", "year", "slug"}, v: []string{"456", "09", "04", "1984", "juice"}},
		{r: "/articles/456.json", h: hStub10, k: []string{"id"}, v: []string{"456"}},
		{r: "/articles/456/data.json", h: hStub11, k: []string{"id"}, v: []string{"456"}},

		{r: "/articles/files/file.zip", h: hStub12, k: []string{"file", "ext"}, v: []string{"file", "zip"}},
		{r: "/articles/files/photos.tar.gz", h: hStub12, k: []string{"file", "ext"}, v: []string{"photos", "tar.gz"}},
		{r: "/articles/files/photos.tar.gz", h: hStub12, k: []string{"file", "ext"}, v: []string{"photos", "tar.gz"}},

		{r: "/pages", h: nil, k: nil, v: nil},
		{r: "/pages/", h: hStub9, k: []string{"*"}, v: []string{""}},
		{r: "/pages/yes", h: hStub9, k: []string{"*"}, v: []string{"yes"}},

		{r: "/users/1", h: hStub14, k: []string{"id"}, v: []string{"1"}},
		{r: "/users/", h: nil, k: nil, v: nil},
		{r: "/users/2/settings/password", h: hStub15, k: []string{"id", "key"}, v: []string{"2", "password"}},
		{r: "/users/2/settings/", h: hStub16, k: []string{"id", "*"}, v: []string{"2", ""}},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, handlers, _ := tr.FindTopic(rctx, tt.r)

		var handler http.Handler
		if handlers != nil {
			handler = handlers.handler
		}

		paramKeys := rctx.topicParams.Keys
		paramValues := rctx.topicParams.Values

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
		require.Equal(t, tt.k, paramKeys, "index %d", i)
		require.Equal(t, tt.v, paramValues, "index %d", i)
	}
}

func TestTreeRegexp(t *testing.T) {
	hStub1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub4 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub5 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub6 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub7 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	tr := &Node[http.Handler]{}
	tr.InsertTopic("/articles/{rid:^[0-9]{5,6}}", hStub7)
	tr.InsertTopic("/articles/{zid:^0[0-9]+}", hStub3)
	tr.InsertTopic("/articles/{name:^@[a-z]+}/posts", hStub4)
	tr.InsertTopic("/articles/{op:^[0-9]+}/run", hStub5)
	tr.InsertTopic("/articles/{id:^[0-9]+}", hStub1)
	tr.InsertTopic("/articles/{id:^[1-9]+}-{aux}", hStub6)
	tr.InsertTopic("/articles/{slug}", hStub2)

	tests := []struct {
		r string       // input request path
		h http.Handler // output matched handler
		k []string     // output param keys
		v []string     // output param values
	}{
		{r: "/articles", h: nil, k: nil, v: nil},
		{r: "/articles/12345", h: hStub7, k: []string{"rid"}, v: []string{"12345"}},
		{r: "/articles/123", h: hStub1, k: []string{"id"}, v: []string{"123"}},
		{r: "/articles/how-to-build-a-router", h: hStub2, k: []string{"slug"}, v: []string{"how-to-build-a-router"}},
		{r: "/articles/0456", h: hStub3, k: []string{"zid"}, v: []string{"0456"}},
		{r: "/articles/@pk/posts", h: hStub4, k: []string{"name"}, v: []string{"@pk"}},
		{r: "/articles/1/run", h: hStub5, k: []string{"op"}, v: []string{"1"}},
		{r: "/articles/1122", h: hStub1, k: []string{"id"}, v: []string{"1122"}},
		{r: "/articles/1122-yes", h: hStub6, k: []string{"id", "aux"}, v: []string{"1122", "yes"}},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, handlers, _ := tr.FindTopic(rctx, tt.r)

		var handler http.Handler
		if handlers != nil {
			handler = handlers.handler
		}

		paramKeys := rctx.topicParams.Keys
		paramValues := rctx.topicParams.Values

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
		require.Equal(t, tt.k, paramKeys, "index %d", i)
		require.Equal(t, tt.v, paramValues, "index %d", i)
	}
}

func TestTreeRegexpRecursive(t *testing.T) {
	hStub1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	hStub2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	tr := &Node[http.Handler]{}
	tr.InsertTopic("/one/{firstId:[a-z0-9-]+}/{secondId:[a-z0-9-]+}/first", hStub1)
	tr.InsertTopic("/one/{firstId:[a-z0-9-_]+}/{secondId:[a-z0-9-_]+}/second", hStub2)

	tests := []struct {
		r string       // input request path
		h http.Handler // output matched handler
		k []string     // output param keys
		v []string     // output param values
	}{
		{r: "/one/hello/world/first", h: hStub1, k: []string{"firstId", "secondId"}, v: []string{"hello", "world"}},
		{r: "/one/hi_there/ok/second", h: hStub2, k: []string{"firstId", "secondId"}, v: []string{"hi_there", "ok"}},
		{r: "/one///first", h: nil, k: nil, v: []string{}},
		{r: "/one/hi/123/second", h: hStub2, k: []string{"firstId", "secondId"}, v: []string{"hi", "123"}},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, handlers, _ := tr.FindTopic(rctx, tt.r)

		var handler http.Handler
		if handlers != nil {
			handler = handlers.handler
		}

		paramKeys := rctx.topicParams.Keys
		paramValues := rctx.topicParams.Values

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
		require.Equal(t, tt.k, paramKeys, "index %d", i)
		require.Equal(t, tt.v, paramValues, "index %d", i)
	}
}

func TestTreeRegexMatchWholeParam(t *testing.T) {
	hStub1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	rctx := NewTopicContext()
	tr := &Node[http.Handler]{}
	tr.InsertTopic("/{id:[0-9]+}", hStub1)
	tr.InsertTopic("/{x:.+}/foo", hStub1)
	tr.InsertTopic("/{param:[0-9]*}/test", hStub1)

	tests := []struct {
		expectedHandler http.Handler
		url             string
	}{
		{url: "/13", expectedHandler: hStub1},
		{url: "/a13", expectedHandler: nil},
		{url: "/13.jpg", expectedHandler: nil},
		{url: "/a13.jpg", expectedHandler: nil},
		{url: "/a/foo", expectedHandler: hStub1},
		{url: "//foo", expectedHandler: nil},
		{url: "//test", expectedHandler: hStub1},
	}

	for i, tc := range tests {
		_, _, handler := tr.FindTopic(rctx, tc.url)
		require.Equal(t, fmt.Sprintf("%v", tc.expectedHandler), fmt.Sprintf("%v", handler), "index %d", i)
	}
}

func TestFindTopic(t *testing.T) {
	type Handler func()
	h1 := func() {}
	tr := &Node[Handler]{}
	tr.InsertTopic("test", h1)
	tr.InsertTopic("device/to/{cId}/{module}", h1)

	tests := []struct {
		r string  // input request path
		h Handler // output matched handler
	}{
		{"test", h1},
		{"test2", nil},
		{"device/to/abc", nil},
		{"device/to/123/abc", h1},
		{"device/to/123/abc/cde", nil},
		{"/device/to/123/abc/cde", nil},
	}

	for i, tt := range tests {
		rctx := NewTopicContext()

		_, _, handler := tr.FindTopic(rctx, tt.r)

		require.Equal(t, fmt.Sprintf("%v", tt.h), fmt.Sprintf("%v", handler), "index %d", i)
	}
}

func BenchmarkTreeGet(b *testing.B) {
	h1 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h2 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	tr := &Node[http.Handler]{}
	tr.InsertTopic("test/fr/", h1)
	tr.InsertTopic("test/chi/{ida}/{idb}", h1)
	tr.InsertTopic("SomeFeature/chi/", h2)

	mctx := NewTopicContext()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mctx.Reset()
		_, _, handler := tr.FindTopic(mctx, "test/chi/123/456")
		_ = handler
	}
}

func BenchmarkMapGet(b *testing.B) {
	t := true
	f := false
	topics := make(map[string]*bool)
	topics["test/chi"] = &t
	topics["SomeFeature/chi/"] = &f

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := strings.SplitN("si/chi/123/456", "/chi/", 2)
		p := topics[topic[0]]
		_ = p
		fields := strings.Split(topic[1], "/")
		_ = fields[0]
		_ = fields[1]
	}
}
