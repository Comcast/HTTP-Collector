package radix

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestContext_Param(t *testing.T) {
	testCases := []struct {
		name     string
		keys     []string
		values   []string
		search   string
		expected string
	}{
		{
			name:     "Existing key, cID",
			keys:     []string{"cID", "module"},
			values:   []string{"abc", "123"},
			search:   "cID",
			expected: "abc",
		},
		{
			name:     "Existing key, module",
			keys:     []string{"cID", "module"},
			values:   []string{"abc", "123"},
			search:   "module",
			expected: "123",
		},
		{
			name:     "Non-existing key",
			keys:     []string{"key1", "key2"},
			values:   []string{"value1", "value2"},
			search:   "cID",
			expected: "",
		},
		{
			name:     "Empty topic params",
			keys:     []string{},
			values:   []string{},
			search:   "cID",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &Context{
				TopicParams: TopicParams{
					Keys:   tc.keys,
					Values: tc.values,
				},
			}

			// Call Param with the search key
			value := ctx.Param(tc.search)

			require.Equal(t, tc.expected, value)
		})
	}
}

func TestTopicParams_Add(t *testing.T) {
	testCases := []struct {
		name     string
		initial  TopicParams
		key      string
		value    string
		expected TopicParams
	}{
		{
			name: "Add first key-value pair",
			initial: TopicParams{
				Keys:   []string{},
				Values: []string{},
			},
			key:   "cID",
			value: "abc",
			expected: TopicParams{
				Keys:   []string{"cID"},
				Values: []string{"abc"},
			},
		},
		{
			name: "Add second key-value pair",
			initial: TopicParams{
				Keys:   []string{"cID"},
				Values: []string{"abc"},
			},
			key:   "module",
			value: "123",
			expected: TopicParams{
				Keys:   []string{"cID", "module"},
				Values: []string{"abc", "123"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a new TopicParams and call Add
			tp := tc.initial
			tp.Add(tc.key, tc.value)
			require.Equal(t, tc.expected, tp)
		})
	}
}

func TestContext_Reset(t *testing.T) {
	ctx := &Context{
		TopicPatterns: []string{"pattern1", "pattern2"},
		TopicParams: TopicParams{
			Keys:   []string{"cID", "module"},
			Values: []string{"abc", "123"},
		},
		topicPattern: "pattern",
		topicParams: TopicParams{
			Keys:   []string{"cID"},
			Values: []string{"abc"},
		},
		parentCtx: context.TODO(),
	}

	ctx.Reset()

	// Assert that the state is reset
	require.Equal(t, 0, len(ctx.TopicPatterns))
	require.Equal(t, 0, len(ctx.TopicParams.Keys))
	require.Equal(t, 0, len(ctx.TopicParams.Values))
	require.Equal(t, "", ctx.topicPattern)
	require.Equal(t, 0, len(ctx.topicParams.Keys))
	require.Equal(t, 0, len(ctx.topicParams.Values))
	require.Nil(t, ctx.parentCtx)
}
