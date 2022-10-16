package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/gimlet"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
)

type jobStatusResponse struct {
	Exists      bool        `bson:"job_exists" json:"job_exists" yaml:"job_exists"`
	Completed   bool        `bson:"completed" json:"completed" yaml:"completed"`
	ID          string      `bson:"id,omitempty" json:"id,omitempty" yaml:"id,omitempty"`
	JobsPending int         `bson:"jobs_pending,omitempty" json:"jobs_pending,omitempty" yaml:"jobs_pending,omitempty"`
	Error       string      `bson:"error,omitempty" json:"error,omitempty" yaml:"error,omitempty"`
	Job         interface{} `bson:"job,omitempty" json:"job,omitempty" yaml:"job,omitempty"`
}

func (s *QueueService) getJobStatusResponse(ctx context.Context, name string) (*jobStatusResponse, error) {
	var msg string
	var err error

	resp := &jobStatusResponse{}
	resp.JobsPending = s.queue.Stats(ctx).Pending
	resp.ID = name

	if name == "" {
		msg = fmt.Sprintf("did not specify job name: %s", name)
		err = errors.New(msg)
		resp.Error = msg

		return resp, err
	}

	j, err := s.queue.Get(ctx, name)
	resp.Exists = !amboy.IsJobNotDefinedError(err)

	if !resp.Exists {
		msg = fmt.Sprintf("could not recover job %q", name)
		err = errors.New(msg)
		resp.Error = msg

		return resp, err
	}

	if err != nil {
		resp.Exists = false
		resp.Error = err.Error()
		return resp, err
	}

	resp.Exists = true
	resp.Completed = j.Status().Completed
	resp.Job = j

	return resp, nil
}

// JobStatus is a http.HandlerFunc that writes a job status document to the request.
func (s *QueueService) JobStatus(w http.ResponseWriter, r *http.Request) {
	name := gimlet.GetVars(r)["name"]

	response, err := s.getJobStatusResponse(r.Context(), name)
	if err != nil {
		grip.Error(err)
		gimlet.WriteJSONError(w, response)
		return
	}

	gimlet.WriteJSON(w, response)
}

// WaitJob waits for a single job to be complete. It takes a timeout
// argument, which defaults to 10 seconds, and returns 408 (request
// timeout) if the timeout is reached before the job completes.
func (s *QueueService) WaitJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := gimlet.GetVars(r)["name"]
	response, err := s.getJobStatusResponse(ctx, name)
	if err != nil {
		grip.Error(err)
		gimlet.WriteJSONError(w, response)
	}

	timeout, err := parseTimeout(r)
	if err != nil {
		grip.Info(message.WrapError(err, message.Fields{
			"message": "problem parsing timeout",
			"name":    name,
		}))
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	response, code, err := s.waitForJob(ctx, name)
	grip.Error(err)
	gimlet.WriteJSONResponse(w, code, response)
}

func parseTimeout(r *http.Request) (time.Duration, error) {
	var err error

	timeout := 10 * time.Second

	timeoutInput, ok := r.URL.Query()["timeout"]

	if ok || len(timeoutInput) != 0 {
		timeout, err = time.ParseDuration(timeoutInput[0])
		if err != nil {
			timeout = 10 * time.Second
		}
	}

	return timeout, fmt.Errorf("problem parsing timeout from %s: %w", timeoutInput, err)
}

func (s *QueueService) waitForJob(ctx context.Context, name string) (*jobStatusResponse, int, error) {
	job, err := s.queue.Get(ctx, name)
	if err != nil {
		response, err2 := s.getJobStatusResponse(ctx, name)
		grip.Error(err2)
		if amboy.IsJobNotDefinedError(err) {
			return response, http.StatusNotFound, fmt.Errorf(
				"problem finding job: %s", name)
		}

		return response, http.StatusInternalServerError, fmt.Errorf(
			"problem retrieving job: %s", name)
	}

	ok := amboy.WaitJobInterval(ctx, job, s.queue, 100*time.Millisecond)

	response, err := s.getJobStatusResponse(ctx, name)
	if err != nil {
		return response, http.StatusInternalServerError, fmt.Errorf("problem constructing response for while waiting for job %s: %w", name, err)
	}

	if !ok {
		return response, http.StatusRequestTimeout, fmt.Errorf(
			"reached timeout waiting for job: %s", name)
	}

	return response, http.StatusOK, nil
}

func (s *QueueService) markTypeCompleteLocal(jobType string) {
}
