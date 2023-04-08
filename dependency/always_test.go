package dependency

import (
	"testing"
)

// AlwaysRebuildSuite tests the Always dependency implementation which
// always returns the Ready dependency state. Does contain support for
// dependency graph resolution, but the tasks will always run.
type AlwaysRebuildSuite struct {
}

func TestAlwaysRebuild(t *testing.T) {
	for name, tfunc := range map[string]func(t *testing.T, dep *alwaysManager){
		"Implements": func(t *testing.T, dep *alwaysManager) {
			var mgr Manager
			mgr = dep
		},
		"TypeName": func(t *testing.T, dep *alwaysManager) {
			if dep.T.Name != "always" {
				t.Error(dep.T.Name)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dep := &alwaysManager{}
			tfunc(t, dep)
		})
	}
}

func (s *AlwaysRebuildSuite) TestHasComposedJobEdgesInstance() {
	s.IsType(s.dep.JobEdges, JobEdges{})

	var ok bool
	var dep interface{} = s.dep

	_, ok = dep.(interface {
		Edges() []string // nolint
	})

	s.True(ok)

	_, ok = dep.(interface {
		AddEdge(string) error // nolint
	})

	s.True(ok)
}

func (s *AlwaysRebuildSuite) TestTypeAccessorProvidesAccessToTheCorrectTypeInfo() {
	s.Equal("always", s.dep.Type().Name)
	s.Equal(0, s.dep.Type().Version)
}
