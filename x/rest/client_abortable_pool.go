package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/tychoish/gimlet"
)

// AbortablePoolManagementClient provides a go wrapper to the
// AbortablePoolManagement service.
type AbortablePoolManagementClient struct {
	client *http.Client
	url    string
}

// NewAbortablePoolManagementClient constructs a new
// AbortablePoolManagementClient instance that constructs a new http.Client.
func NewAbortablePoolManagementClient(url string) *AbortablePoolManagementClient {
	return NewAbortablePoolManagementClientFromExisting(&http.Client{}, url)
}

// NewAbortablePoolManagementClientFromExisting builds an
// AbortablePoolManagementClient instance from an existing http.Client.
func NewAbortablePoolManagementClientFromExisting(client *http.Client, url string) *AbortablePoolManagementClient {
	return &AbortablePoolManagementClient{
		client: client,
		url:    url,
	}
}

// ListJobs returns a full list of all running jobs managed by the
// pool that the service reflects.
func (c *AbortablePoolManagementClient) ListJobs(ctx context.Context) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, c.url+"/v1/jobs/list", nil)
	if err != nil {
		return nil, fmt.Errorf("problem building request: %w", err)
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error processing request: %w", err)
	}
	defer resp.Body.Close()
	out := []string{}
	if err = gimlet.GetJSON(resp.Body, &out); err != nil {
		return nil, fmt.Errorf("problem reading response: %w", err)
	}

	return out, nil
}

// AbortAllJobs issues the request to terminate all currently running
// jobs managed by the pool that backs the request.
func (c *AbortablePoolManagementClient) AbortAllJobs(ctx context.Context) error {
	req, err := http.NewRequest(http.MethodDelete, c.url+"/v1/jobs/abort", nil)
	if err != nil {
		return fmt.Errorf("problem building request: %w", err)
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error processing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to abort jobs")
	}

	return nil
}

// IsRunning checks if a job with a specified id is currently running
// in the remote queue. Check the error value to identify if false
// response is due to a communication problem with the service or is
// legitimate.
func (c *AbortablePoolManagementClient) IsRunning(ctx context.Context, job string) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, c.url+"/v1/jobs/"+job, nil)
	if err != nil {
		return false, fmt.Errorf("problem building request: %w", err)
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error processing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return true, nil
}

// AbortJob sends the abort signal for a running job to the management
// service, return any errors from the service. A nil response
// indicates that the job has been successfully terminated.
func (c *AbortablePoolManagementClient) AbortJob(ctx context.Context, job string) error {
	req, err := http.NewRequest(http.MethodDelete, c.url+"/v1/jobs/"+job, nil)
	if err != nil {
		return fmt.Errorf("problem building request: %w", err)
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error processing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		rerr := &gimlet.ErrorResponse{}
		if err := gimlet.GetJSON(resp.Body, rerr); err != nil {
			return fmt.Errorf("problem reading error response with %s: %w", http.StatusText(resp.StatusCode), err)

		}
		return fmt.Errorf("remove server returned error: %w", rerr)
	}

	return nil
}
