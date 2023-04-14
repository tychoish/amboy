package registry

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/dependency"
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
				if err := NewJobResolutionError(amboy.JobType{Version: i}, i+1); err == nil {
					t.Fatal("expected error")
				}
				if err := NewDependencyResolutionError(dependency.TypeInfo{Version: i}, i+1); err == nil {
					t.Fatal("expected error")
				}
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
			if err == nil {
				t.Fatal("expected error")
			}
			require.False(t, IsVersionResolutionError(err))
		})
		t.Run("MismatchedVersion", func(t *testing.T) {
			err := &resolutionError{RegisteredVersion: 1, RecordVersion: 2}
			if err == nil {
				t.Fatal("expected error")
			}
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
