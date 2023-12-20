package radix

// Radix tree implementation below is based on chi tree: https://github.com/go-chi/chi
// Radix tree implementation below is a based on the original work by
// Armon Dadgar in https://github.com/armon/go-radix/blob/master/radix.go
// (MIT licensed). It's been heavily modified for use as a HTTP routing tree.

import (
	"errors"
	"regexp"
	"sort"
	"strings"
)

type nodeTyp uint8

const (
	ntStatic   nodeTyp = iota // /home
	ntRegexp                  // /{id:[0-9]+}
	ntParam                   // /{user}
	ntCatchAll                // /api/v1/*
)

type Node[T any] struct {
	// regexp matcher for regexp nodes
	rex *regexp.Regexp

	// HTTP handler endpoints on the leaf node
	endpoint *endpoint[T]

	// prefix is the common prefix we ignore
	prefix string

	// child nodes should be stored in-order for iteration,
	// in groups of the node type.
	children [ntCatchAll + 1]nodes[T]

	// first byte of the child prefix
	tail byte

	// node type: static, regexp, param, catchAll
	typ nodeTyp

	// first byte of the prefix
	label byte
}

type endpoint[T any] struct {
	// endpoint handler
	handler T

	// pattern is the routing pattern for handler nodes
	pattern string

	// parameter keys recorded on handler nodes
	paramKeys []string
}

func New[T any]() *Node[T] {
	return new(Node[T])
}

func (n *Node[T]) InsertTopic(pattern string, handler T) (*Node[T], error) {
	var parent *Node[T]
	search := pattern

	for {
		// Handle key exhaustion
		if len(search) == 0 {
			// Insert or update the node's leaf handler
			err := n.setEndpoint(handler, pattern)
			return n, err
		}

		// We're going to be searching for a wild node next,
		// in this case, we need to get the tail
		var label = search[0]
		var segTail byte
		var segEndIdx int
		var segTyp nodeTyp
		var segRexpat string
		if label == '{' || label == '*' {
			var err error
			segTyp, _, segRexpat, segTail, _, segEndIdx, err = patNextSegment(search)
			if err != nil {
				return nil, err
			}
		}

		var prefix string
		if segTyp == ntRegexp {
			prefix = segRexpat
		}

		// Look for the edge to attach to
		parent = n
		n = n.getEdge(segTyp, label, segTail, prefix)

		// No edge, create one
		if n == nil {
			child := &Node[T]{label: label, tail: segTail, prefix: search}
			hn, err := parent.addChild(child, search)
			if err != nil {
				return nil, err
			}
			err = hn.setEndpoint(handler, pattern)

			return hn, err
		}

		// Found an edge to match the pattern

		if n.typ > ntStatic {
			// We found a param node, trim the param from the search path and continue.
			// This param/wild pattern segment would already be on the tree from a previous
			// call to addChild when creating a new node.
			search = search[segEndIdx:]
			continue
		}

		// Static nodes fall below here.
		// Determine longest prefix of the search key on match.
		commonPrefix := longestPrefix(search, n.prefix)
		if commonPrefix == len(n.prefix) {
			// the common prefix is as long as the current node's prefix we're attempting to insert.
			// keep the search going.
			search = search[commonPrefix:]
			continue
		}

		// Split the node
		child := &Node[T]{
			typ:    ntStatic,
			prefix: search[:commonPrefix],
		}
		err := parent.replaceChild(search[0], segTail, child)
		if err != nil {
			return nil, err
		}

		// Restore the existing node
		n.label = n.prefix[commonPrefix]
		n.prefix = n.prefix[commonPrefix:]
		child.addChild(n, n.prefix)

		// If the new key is a subset, set the method/handler on this node and finish.
		search = search[commonPrefix:]
		if len(search) == 0 {
			err := child.setEndpoint(handler, pattern)
			return child, err
		}

		// Create a new edge for the node
		subchild := &Node[T]{
			typ:    ntStatic,
			label:  search[0],
			prefix: search,
		}
		hn, err := child.addChild(subchild, search)
		if err != nil {
			return nil, err
		}
		err = hn.setEndpoint(handler, pattern)
		return hn, err
	}
}

// addChild appends the new `child` node to the tree using the `pattern` as the trie key.
// For a URL router like chi's, we split the static, param, regexp and wildcard segments
// into different nodes. In addition, addChild will recursively call itself until every
// pattern segment is added to the url pattern tree as individual nodes, depending on type.
func (n *Node[T]) addChild(child *Node[T], prefix string) (*Node[T], error) {
	search := prefix

	// handler leaf node added to the tree is the child.
	// this may be overridden later down the flow
	hn := child

	// Parse next segment
	segTyp, _, segRexpat, segTail, segStartIdx, segEndIdx, err := patNextSegment(search)
	if err != nil {
		return nil, err
	}

	// Add child depending on next up segment
	switch segTyp {

	case ntStatic:
		// Search prefix is all static (that is, has no params in path)
		// noop

	default:
		var err error
		// Search prefix contains a param, regexp or wildcard

		if segTyp == ntRegexp {
			rex, err := regexp.Compile(segRexpat)
			if err != nil {
				return nil, errors.New("invlid regex pattern " + segRexpat)
			}
			child.prefix = segRexpat
			child.rex = rex
		}

		if segStartIdx == 0 {
			// Route starts with a param
			child.typ = segTyp

			if segTyp == ntCatchAll {
				segStartIdx = -1
			} else {
				segStartIdx = segEndIdx
			}
			if segStartIdx < 0 {
				segStartIdx = len(search)
			}
			child.tail = segTail // for params, we set the tail

			if segStartIdx != len(search) {
				// add static edge for the remaining part, split the end.
				// its not possible to have adjacent param nodes, so its certainly
				// going to be a static node next.

				search = search[segStartIdx:] // advance search position

				nn := &Node[T]{
					typ:    ntStatic,
					label:  search[0],
					prefix: search,
				}
				hn, err = child.addChild(nn, search)
				if err != nil {
					return nil, err
				}
			}

		} else if segStartIdx > 0 {
			// Route has some param

			// starts with a static segment
			child.typ = ntStatic
			child.prefix = search[:segStartIdx]
			child.rex = nil

			// add the param edge node
			search = search[segStartIdx:]

			nn := &Node[T]{
				typ:   segTyp,
				label: search[0],
				tail:  segTail,
			}
			hn, err = child.addChild(nn, search)
			if err != nil {
				return nil, err
			}
		}
	}

	n.children[child.typ] = append(n.children[child.typ], child)
	n.children[child.typ].Sort()
	return hn, nil
}

func (n *Node[T]) replaceChild(label, tail byte, child *Node[T]) error {
	for i := 0; i < len(n.children[child.typ]); i++ {
		if n.children[child.typ][i].label == label && n.children[child.typ][i].tail == tail {
			n.children[child.typ][i] = child
			n.children[child.typ][i].label = label
			n.children[child.typ][i].tail = tail
			return nil
		}
	}
	return errors.New("replacing missing child")
}

func (n *Node[T]) getEdge(ntyp nodeTyp, label, tail byte, prefix string) *Node[T] {
	nds := n.children[ntyp]
	for i := 0; i < len(nds); i++ {
		if nds[i].label == label && nds[i].tail == tail {
			if ntyp == ntRegexp && nds[i].prefix != prefix {
				continue
			}
			return nds[i]
		}
	}
	return nil
}

func (n *Node[T]) setEndpoint(handler T, pattern string) error {
	paramKeys, err := PatParamKeys(pattern)
	if err != nil {
		return err
	}

	n.endpoint = &endpoint[T]{
		handler:   handler,
		pattern:   pattern,
		paramKeys: paramKeys,
	}
	return nil
}

func (n *Node[T]) FindTopic(rctx *Context, path string) (*Node[T], *endpoint[T], T) {
	// Reset the context routing pattern and params
	rctx.topicPattern = ""
	rctx.topicParams.Keys = rctx.topicParams.Keys[:0]
	rctx.topicParams.Values = rctx.topicParams.Values[:0]

	// Find the routing handlers for the path
	rn := n.findTopic(rctx, path)
	if rn == nil {
		var empty T
		return nil, nil, empty
	}

	// Record the routing params in the request lifecycle
	rctx.TopicParams.Keys = append(rctx.TopicParams.Keys, rctx.topicParams.Keys...)
	rctx.TopicParams.Values = append(rctx.TopicParams.Values, rctx.topicParams.Values...)

	// Record the routing pattern in the request lifecycle
	if rn.endpoint.pattern != "" {
		rctx.topicPattern = rn.endpoint.pattern
		rctx.TopicPatterns = append(rctx.TopicPatterns, rctx.topicPattern)
	}

	return rn, rn.endpoint, rn.endpoint.handler
}

// Recursive edge traversal by checking all nodeTyp groups along the way.
// It's like searching through a multi-dimensional radix trie.
func (n *Node[T]) findTopic(rctx *Context, path string) *Node[T] {
	nn := n
	search := path

	for t, nds := range nn.children {
		if len(nds) == 0 {
			continue
		}

		var xn *Node[T]
		xsearch := search

		var label byte
		if search != "" {
			label = search[0]
		}

		ntyp := nodeTyp(t)
		switch ntyp {
		case ntStatic:
			xn = nds.findEdge(label)
			if xn == nil || !strings.HasPrefix(xsearch, xn.prefix) {
				continue
			}
			xsearch = xsearch[len(xn.prefix):]

		case ntParam, ntRegexp:
			// short-circuit and return no matching route for empty param values
			if xsearch == "" {
				continue
			}

			// serially loop through each node grouped by the tail delimiter
			for idx := 0; idx < len(nds); idx++ {
				xn = nds[idx]

				// label for param nodes is the delimiter byte
				p := strings.IndexByte(xsearch, xn.tail)

				if p < 0 {
					if xn.tail == '/' {
						p = len(xsearch)
					} else {
						continue
					}
				} else if ntyp == ntRegexp && p == 0 {
					continue
				}

				if ntyp == ntRegexp && xn.rex != nil {
					if !xn.rex.MatchString(xsearch[:p]) {
						continue
					}
				} else if strings.IndexByte(xsearch[:p], '/') != -1 {
					// avoid a match across path segments
					continue
				}

				prevlen := len(rctx.topicParams.Values)
				rctx.topicParams.Values = append(rctx.topicParams.Values, xsearch[:p])
				xsearch = xsearch[p:]

				if len(xsearch) == 0 {
					if xn.isLeaf() {
						h := xn.endpoint
						if h != nil {
							rctx.topicParams.Keys = append(rctx.topicParams.Keys, h.paramKeys...)
							return xn
						}

					}
				}

				// recursively find the next node on this branch
				fin := xn.findTopic(rctx, xsearch)
				if fin != nil {
					return fin
				}

				// not found on this branch, reset vars
				rctx.topicParams.Values = rctx.topicParams.Values[:prevlen]
				xsearch = search
			}

			rctx.topicParams.Values = append(rctx.topicParams.Values, "")

		default:
			// catch-all nodes
			rctx.topicParams.Values = append(rctx.topicParams.Values, search)
			xn = nds[0]
			xsearch = ""
		}

		if xn == nil {
			continue
		}

		// did we find it yet?
		if len(xsearch) == 0 {
			if xn.isLeaf() {
				h := xn.endpoint
				if h != nil {
					rctx.topicParams.Keys = append(rctx.topicParams.Keys, h.paramKeys...)
					return xn
				}

			}
		}

		// recursively find the next node..
		fin := xn.findTopic(rctx, xsearch)
		if fin != nil {
			return fin
		}

		// Did not find final handler, let's remove the param here if it was set
		if xn.typ > ntStatic {
			if len(rctx.topicParams.Values) > 0 {
				rctx.topicParams.Values = rctx.topicParams.Values[:len(rctx.topicParams.Values)-1]
			}
		}

	}

	return nil
}

func (n *Node[T]) isLeaf() bool {
	return n.endpoint != nil
}

// patNextSegment returns the next segment details from a pattern:
// node type, param key, regexp string, param tail byte, param starting index, param ending index
func patNextSegment(pattern string) (nodeTyp, string, string, byte, int, int, error) {
	ps := strings.Index(pattern, "{")
	ws := strings.Index(pattern, "*")

	if ps < 0 && ws < 0 {
		return ntStatic, "", "", 0, 0, len(pattern), nil // we return the entire thing
	}

	// Sanity check
	if ps >= 0 && ws >= 0 && ws < ps {
		return 0, "", "", 0, 0, 0, errors.New("wildcard must be the last pattern")
	}

	var tail byte = '/' // Default endpoint tail to / byte

	if ps >= 0 {
		// Param/Regexp pattern is next
		nt := ntParam

		// Read to closing } taking into account opens and closes in curl count (cc)
		cc := 0
		pe := ps
		for i, c := range pattern[ps:] {
			if c == '{' {
				cc++
			} else if c == '}' {
				cc--
				if cc == 0 {
					pe = ps + i
					break
				}
			}
		}
		if pe == ps {
			return 0, "", "", 0, 0, 0, errors.New("closing delimiter '}' is missing")
		}

		key := pattern[ps+1 : pe]
		pe++ // set end to next position

		if pe < len(pattern) {
			tail = pattern[pe]
		}

		var rexpat string
		if idx := strings.Index(key, ":"); idx >= 0 {
			nt = ntRegexp
			rexpat = key[idx+1:]
			key = key[:idx]
		}

		if len(rexpat) > 0 {
			if rexpat[0] != '^' {
				rexpat = "^" + rexpat
			}
			if rexpat[len(rexpat)-1] != '$' {
				rexpat += "$"
			}
		}

		return nt, key, rexpat, tail, ps, pe, nil
	}

	// Wildcard pattern as finale
	if ws < len(pattern)-1 {
		return 0, "", "", 0, 0, 0, errors.New("wildcard must be the last pattern")
	}
	return ntCatchAll, "*", "", 0, ws, len(pattern), nil
}

func PatParamKeys(pattern string) ([]string, error) {
	pat := pattern
	paramKeys := []string{}
	for {
		ptyp, paramKey, _, _, _, e, err := patNextSegment(pat)
		if err != nil {
			return nil, err
		}
		if ptyp == ntStatic {
			return paramKeys, nil
		}
		for i := 0; i < len(paramKeys); i++ {
			if paramKeys[i] == paramKey {
				return nil, errors.New("duplicate param key")
			}
		}
		paramKeys = append(paramKeys, paramKey)
		pat = pat[e:]
	}
}

// longestPrefix finds the length of the shared prefix
// of two strings
func longestPrefix(k1, k2 string) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

type nodes[T any] []*Node[T]

// Sort the list of nodes by label
func (ns nodes[T]) Sort()              { sort.Sort(ns); ns.tailSort() }
func (ns nodes[T]) Len() int           { return len(ns) }
func (ns nodes[T]) Swap(i, j int)      { ns[i], ns[j] = ns[j], ns[i] }
func (ns nodes[T]) Less(i, j int) bool { return ns[i].label < ns[j].label }

// tailSort pushes nodes with '/' as the tail to the end of the list for param nodes.
// The list order determines the traversal order.
func (ns nodes[T]) tailSort() {
	for i := len(ns) - 1; i >= 0; i-- {
		if ns[i].typ > ntStatic && ns[i].tail == '/' {
			ns.Swap(i, len(ns)-1)
			return
		}
	}
}

func (ns nodes[T]) findEdge(label byte) *Node[T] {
	num := len(ns)
	idx := 0
	i, j := 0, num-1
	for i <= j {
		idx = i + (j-i)/2
		if label > ns[idx].label {
			i = idx + 1
		} else if label < ns[idx].label {
			j = idx - 1
		} else {
			i = num // breaks cond
		}
	}
	if ns[idx].label != label {
		return nil
	}
	return ns[idx]
}
