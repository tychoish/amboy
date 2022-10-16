package rest

import (
	"encoding/json"
	"net/http"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/registry"
	"github.com/tychoish/gimlet"
	"github.com/tychoish/grip"
)

// Fetch is an http handler that writes a job interchange object to a
// the response, and allows clients to retrieve jobs from the service.
func (s *QueueService) Fetch(w http.ResponseWriter, r *http.Request) {
	name := gimlet.GetVars(r)["name"]

	job, err := s.queue.Get(r.Context(), name)
	if err != nil {
		grip.Infof("job named %s does not exist in the queue", name)
		if amboy.IsJobNotDefinedError(err) {
			gimlet.WriteJSONResponse(w, http.StatusNotFound, nil)
		} else {
			gimlet.WriteJSONResponse(w, http.StatusInternalServerError, nil)
		}

		return
	}

	resp, err := registry.MakeJobInterchange(job, json.Marshal)
	if err != nil {
		grip.Warningf("problem converting job %s to interchange format", name)
		gimlet.WriteJSONResponse(w, http.StatusInternalServerError, resp)
		return
	}

	gimlet.WriteJSON(w, resp)
}
