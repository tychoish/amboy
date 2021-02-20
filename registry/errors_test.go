package registry

import (
	"testing"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/dependency"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestErrors(t *testing.T) {
	t.Run("Constructor", func(t *testing.T) {
		t.Run("NilIfMatch", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				require.NoError(t, NewJobResolutionError(amboy.JobType{Version: i}, i))
				require.NoError(t, NewDependencyResolutionError(dependency.TypeInfo{Version: i}, i))
			}
		})
		t.Run("ErrorIfNoMatch", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				require.Error(t, NewJobResolutionError(amboy.JobType{Version: i}, i+1))
				require.Error(t, NewDependencyResolutionError(dependency.TypeInfo{Version: i}, i+1))
			}
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Run("Nil", func(t *testing.T) {
			require.False(t, IsVersionResolutionError(nil))
		})
		t.Run("WrongType", func(t *testing.T) {
			require.False(t, IsVersionResolutionError(errors.New("test")))
		})
		t.Run("MatchingVersion", func(t *testing.T) {
			err := &resolutionError{RegisteredVersion: 1, RecordVersion: 1}
			require.Error(t, err)
			require.False(t, IsVersionResolutionError(err))
		})
		t.Run("MismatchedVersion", func(t *testing.T) {
			err := &resolutionError{RegisteredVersion: 1, RecordVersion: 2}
			require.Error(t, err)
			require.True(t, IsVersionResolutionError(err))
		})
		t.Run("RoundTrip", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				require.True(t, IsVersionResolutionError(NewJobResolutionError(amboy.JobType{Version: i}, i+1)))
				require.True(t, IsVersionResolutionError(NewDependencyResolutionError(dependency.TypeInfo{Version: i}, i+1)))
			}
		})
	})
}
