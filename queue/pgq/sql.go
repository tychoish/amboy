package pgq

const bootstrapDB = `
CREATE TABLE IF NOT EXISTS {schemaName}.jobs (
	id text NOT NULL PRIMARY KEY,
	type text NOT NULL,
	queue_group text DEFAULT ''::text NOT NULL,
	version integer NOT NULL,
	priority integer  NOT NULL,
	body jsonb NOT NULL,
	errors text[] NOT NULL,

	started_at timestamptz NOT NULL,
	ended_at timestamptz NOT NULL,
	wait_until timestamptz NOT NULL,
	dispatch_by timestamptz NOT NULL,
	max_time integer NOT NULL,

	owner text NOT NULL,
	completed boolean NOT NULL,
	in_progress boolean NOT NULL,
        canceled boolean NOT NULL,
	mod_count integer NOT NULL,
	err_count integer NOT NULL,

	dep_type text NOT NULL,
	dep_version integer NOT NULL,
	dependency jsonb NOT NULL,
	dep_edges text[] NOT NULL,

	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS {schemaName}.job_scopes (
	id text NOT NULL,
	scope text UNIQUE NOT NULL,
	FOREIGN KEY (id) REFERENCES {schemaName}.jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS jobs_queue_group ON {schemaName}.jobs (queue_group);
CREATE INDEX IF NOT EXISTS jobs_group_type ON {schemaName}.jobs (queue_group, type);
CREATE INDEX IF NOT EXISTS jobs_priority ON {schemaName}.jobs (queue_group, priority);
CREATE INDEX IF NOT EXISTS jobs_completed_in_progress ON {schemaName}.jobs (completed, in_progress);
CREATE INDEX IF NOT EXISTS jobs_completed_in_progress_modtime ON {schemaName}.jobs (completed, in_progress, updated_at);
CREATE INDEX IF NOT EXISTS jobs_end ON {schemaName}.jobs (ended_at);
CREATE INDEX IF NOT EXISTS jobs_created_at ON {schemaName}.jobs (created_at);
CREATE INDEX IF NOT EXISTS jobs_wait_until ON {schemaName}.jobs (wait_until);
CREATE INDEX IF NOT EXISTS jobs_dispatch_by ON {schemaName}.jobs (dispatch_by);
CREATE INDEX IF NOT EXISTS jobs_timing_combined_one ON {schemaName}.jobs (wait_until, dispatch_by);
CREATE INDEX IF NOT EXISTS jobs_timing_combined_two ON {schemaName}.jobs (dispatch_by, wait_until);
CREATE UNIQUE INDEX IF NOT EXISTS job_scopes_name ON {schemaName}.job_scopes (scope);
`

const insertJob = `
INSERT INTO 
	{schemaName}.jobs (
		id, type, queue_group, version, priority, body, errors,
		started_at, ended_at, wait_until, dispatch_by, max_time,
		owner, completed, in_progress, canceled, mod_count, err_count,
		dep_type, dep_version, dependency, dep_edges,
		created_at, updated_at
	)
VALUES
	(
		:id, :type, :queue_group, :version, :priority, :body, :errors,
		:started_at, :ended_at, :wait_until, :dispatch_by, :max_time,
		:owner, :completed, :in_progress, :canceled, :mod_count, :err_count,
		:dep_type, :dep_version, :dependency, :dep_edges,
		:created_at, :updated_at
	)
`

const getActiveGroups = `
SELECT
	DISTINCT queue_group
FROM
	{schemaName}.jobs
WHERE
	(completed = false OR (completed = true AND updated_at >= $1))
	AND queue_group != ''`

const getJobByID = `
SELECT
	*
FROM
	{schemaName}.jobs
WHERE
	jobs.id = $1
`

const updateJob = `
UPDATE
	{schemaName}.jobs
SET
	type = :type,
	queue_group = :queue_group,
	version = :version,
	priority = :priority,
	body = :body,
	owner = :owner,
	completed = :completed,
	canceled = :canceled,
	in_progress = :in_progress,
	mod_count = :mod_count,
	err_count = :err_count,
	started_at = :started_at,
	ended_at = :ended_at,
	wait_until = :wait_until,
	dispatch_by = :dispatch_by,
	max_time = :max_time,
	errors = :errors,
	updated_at = :updated_at,
	created_at = :created_at
WHERE
	id = :id
`

const completeSinglePendingJob = `
UPDATE
	{schemaName}.jobs
SET
	completed = true,
	in_progress = false,
	mod_count = mod_count + 3
WHERE
	id = $1`

const completeManyPendingJobs = `
UPDATE
	{schemaName}.jobs
SET
	completed = true,
	in_progress = false,
	mod_count = mod_count + 3
WHERE
	id IN (?)`

const removeJobScopes = `
DELETE FROM
	{schemaName}.job_scopes
WHERE
	id = $1`

const removeManyJobScopes = `
DELETE FROM
	{schemaName}.job_scopes
WHERE
	id IN (?)
`

const findJobsToCompleteTemplate = `
SELECT
	id
FROM
	{schemaName}.jobs
WHERE
`

const checkCanUpdate = `
SELECT
	COUNT(*)
FROM
	{schemaName}.jobs
WHERE
	id = $1
	AND (	
		(
			owner = $2
			AND mod_count = $3 - 1
			AND updated_at > $4
		)
		OR updated_at <= $4
	)
`

const countTotalJobs = `
SELECT
	COUNT(*)
FROM
	{schemaName}.jobs
WHERE
	jobs.queue_group = $1`

const countPendingJobs = `
SELECT
	COUNT(*)
FROM
	{schemaName}.jobs
WHERE
	queue_group = $1
	AND completed = false`

const countInProgJobs = `
SELECT
	COUNT(*)
FROM
	{schemaName}.jobs
WHERE
	queue_group = $1
	AND completed = false
	AND in_progress = true`

const getAllJobIDs = `
SELECT
	id
FROM
	{schemaName}.jobs
ORDER BY
	updated_at DESC`

const getNextJobsBasic = `
SELECT
	id
FROM
	{schemaName}.jobs
WHERE
	completed = false
	AND queue_group = :group_name
	AND ((in_progress = false) OR (in_progress = true AND updated_at <= :lock_expires))
`

const getNextJobsTimingTemplate = `
SELECT
	id
FROM
	{schemaName}.jobs
WHERE
	completed = false
	AND queue_group = :group_name
	AND ((in_progress = false) OR (in_progress = true AND updated_at <= :lock_expires))
`

const groupJobStatusTemplate = `
SELECT
	COUNT(*) AS count,
	type{{project_group}}
FROM
	{schemaName}.jobs
`

const findJobIDsByStateTemplate = `
SELECT
	id
FROM
	{schemaName}.jobs
WHERE
	type = :job_type
`

const recentTimingTemplate = `
SELECT
	type, {{project_group}}
	AVG((EXTRACT(epoch FROM {{from_time}}) -  EXTRACT(epoch FROM {{to_time}})) * 1000000000) AS duration
FROM
	{schemaName}.jobs
WHERE
`

const recentJobErrorsTemplate = `
SELECT
	type, {{queue_group}}
	COUNT(id) as count,
	SUM(err_count) as total,
	AVG(err_count) as average{{agg_errors}}
FROM
	{schemaName}.jobs
WHERE
	completed = true
	AND err_count > 0
	AND ended_at > :window
`

const pruneJobsQueryTemplate = `
DELETE FROM
	{schemaName}.jobs
WHERE
	id IN (
		SELECT
			id
		FROM
			{schemaName}.jobs
		WHERE
			{{match}}{{limit}}
	)
RETURNING id`
