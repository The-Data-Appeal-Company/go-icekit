package kit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

type fakeTerminator struct {
	name   string
	calls  *[]string
	retErr error
}

func (f fakeTerminator) Terminate(_ context.Context, _ ...testcontainers.TerminateOption) error {
	*f.calls = append(*f.calls, "terminate:"+f.name)
	return f.retErr
}

type fakeCloser struct {
	calls  *[]string
	retErr error
}

func (f fakeCloser) Close() error {
	*f.calls = append(*f.calls, "db:close")
	return f.retErr
}

type fakePointerCloser struct {
	calls *[]string
}

func (f *fakePointerCloser) Close() error {
	*f.calls = append(*f.calls, "db:close")
	return nil
}

type fakeNetwork struct {
	calls  *[]string
	retErr error
}

func (f fakeNetwork) Remove(_ context.Context) error {
	*f.calls = append(*f.calls, "network:remove")
	return f.retErr
}

func TestRollbackSetup_IntermediateFailure_CleansAllInReverseOrder(t *testing.T) {
	ctx := context.Background()
	var calls []string

	resources := []resourceTerminator{
		fakeTerminator{name: "postgres", calls: &calls},
		fakeTerminator{name: "minio", calls: &calls},
		fakeTerminator{name: "rest", calls: &calls},
	}

	err := rollbackSetup(
		ctx,
		fakeCloser{calls: &calls},
		fakeNetwork{calls: &calls},
		resources,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{
		"db:close",
		"terminate:rest",
		"terminate:minio",
		"terminate:postgres",
		"network:remove",
	}, calls)
}

func TestRollbackSetup_CleanupContinuesAndAggregatesErrors(t *testing.T) {
	ctx := context.Background()
	var calls []string

	errBoom := errors.New("boom")
	errNet := errors.New("net")

	resources := []resourceTerminator{
		fakeTerminator{name: "postgres", calls: &calls},
		fakeTerminator{name: "minio", calls: &calls, retErr: errBoom},
		fakeTerminator{name: "rest", calls: &calls},
	}

	err := rollbackSetup(
		ctx,
		fakeCloser{calls: &calls, retErr: errors.New("db")},
		fakeNetwork{calls: &calls, retErr: errNet},
		resources,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db")
	assert.Contains(t, err.Error(), "boom")
	assert.Contains(t, err.Error(), "net")
	assert.Equal(t, []string{
		"db:close",
		"terminate:rest",
		"terminate:minio",
		"terminate:postgres",
		"network:remove",
	}, calls)
}

func TestRollbackSetup_TypedNilCloserIsIgnored(t *testing.T) {
	ctx := context.Background()
	var calls []string
	var db *fakePointerCloser

	err := rollbackSetup(
		ctx,
		db,
		fakeNetwork{calls: &calls},
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{
		"network:remove",
	}, calls)
}
