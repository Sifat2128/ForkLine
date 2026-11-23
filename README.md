Forkline --- Technical Project Documentation
Project: Forkline
Description: A persistent, multiprocess job scheduler
Current implementation snapshot: Python package + PostgreSQL +
SQLAlchemy ORM + CLI + subprocess-based workers
Document purpose: Explain the project's purpose, what makes it
useful, the implementation completed so far, the relationships between
source files, the design decisions, the tests performed manually, and
the remaining work required to reach the intended final project.
Accuracy note: This document describes the source code and
implementation history shared in our conversation. It distinguishes
implemented behavior from planned work. It is not claiming that every
planned feature is already complete. No formal automated test suite or
README currently exists.

1. What is Forkline?
Forkline is a persistent job scheduler. It accepts a task from a user,
stores that task in PostgreSQL, and allows one or more worker processes
to claim and execute queued tasks independently of the command-line
process that submitted them.

A simple example:
1. A user submits a wait task through the forkline CLI.
2. Forkline validates the task input.
3. The job is inserted into PostgreSQL with status QUEUED.
4. A worker claims the job and changes its status to RUNNING.
5. The worker launches a separate Python process to execute the task.
6. When execution finishes, the worker records COMPLETED, FAILED,
   or TIMED_OUT.
7. The user can inspect the job later using the CLI because its state
   is stored persistently in PostgreSQL.
The key idea is that submitting a job and executing a job are separate
operations. The CLI does not need to remain alive while a task runs.
The database is the shared source of truth between the CLI, workers, and
recovery logic.
The problem it addresses
Running a task directly inside a web request or CLI command is often
unsuitable for work that may take a long time. The caller may
disconnect, the process may exit, or multiple tasks may need to run
concurrently. A scheduler provides a controlled path for accepting,
tracking, claiming, executing, and recovering work.

Forkline is intended to demonstrate these backend and operating-system
concepts in one coherent system:
- Persistent job storage
- Producer/worker separation
- Multiple independent worker processes
- Safe job claiming under concurrency
- Process isolation for task execution
- Execution timeouts and process termination
- Worker heartbeats and offline detection
- Recovery of jobs left running by workers that appear to have died
- Graceful worker shutdown
- A command-line interface for operating and observing the system
2. Why is Forkline distinctive?
Forkline is not merely a script that runs functions one after another.
Its value is the combination of several mechanisms.
2.1 Persistent state
Jobs are stored in PostgreSQL rather than only in Python memory. The
scheduler can therefore retain job records after the CLI or a worker
process exits. A user can submit a job in one command and inspect its
status in another command.
2.2 Separate producer and consumer roles
The CLI acts as a producer: it validates and stores jobs. Workers act as
consumers: they claim and execute jobs. PostgreSQL connects these
components without requiring them to share Python memory.
2.3 Concurrent-safe job claiming
claim_next_job() uses PostgreSQL row locking with
FOR UPDATE SKIP LOCKED. This is important when more than one worker is
running. A worker locks a candidate queued row while claiming it;
another worker can skip that locked row and claim a different available
job rather than waiting for the first worker.

The current claim order is ascending job ID, so the implementation
behaves as FIFO by insertion order. Although jobs have a priority
column, priority is not currently used when choosing the next job.
2.4 Process isolation
A worker does not call the task function directly in its own process. It
starts forkline.task_runner as a separate Python subprocess. This
isolates task execution from the worker's own Python process and allows
the worker to monitor the child process, impose a timeout, and inspect
its exit code.
2.5 Recovery mechanisms
Workers periodically update heartbeat timestamps. If a worker's
heartbeat becomes older than the configured threshold, Forkline can mark
it offline and requeue jobs still recorded as running under that worker.

This is a useful crash-recovery mechanism, but it is not a guarantee of
exactly-once execution. A delayed worker might still be alive and
executing after its heartbeat is considered stale. Requeuing its job can
therefore lead to duplicate task side effects. This limitation must be
addressed before describing Forkline as production-safe.
2.6 Educational and engineering value
The project brings together database transactions, concurrency control,
process management, IPC boundaries, failure handling, and operational
tooling. It is a strong systems/backend project because the difficult
part is not just executing a function; it is coordinating state and
processes when things go wrong.
3. High-level architecture
                 User
                  |
                  v
          +---------------+
          |  Forkline CLI |
          |  cli.py       |
          +-------+-------+
                  |
                  v
          +---------------+
          | job_service   |
          | submit/list/  |
          | get/cancel    |
          +-------+-------+
                  |
                  v
          +---------------+
          |  PostgreSQL   |
          | jobs/workers  |
          +-------+-------+
                  ^
                  |
        +---------+----------+
        |                    |
        |                    |
+-------+--------+   +-------+--------+
| Worker process |   | Worker process |
| worker.py      |   | worker.py      |
+-------+--------+   +-------+--------+
        |                    |
        v                    v
+----------------+   +----------------+
| task_runner    |   | task_runner    |
| subprocess     |   | subprocess     |
+-------+--------+   +-------+--------+
        |                    |
        v                    v
+----------------+   +----------------+
| executor.py    |   | executor.py    |
| tasks.py       |   | tasks.py       |
+----------------+   +----------------+
The diagram shows the logical relationships. The current implementation
can run multiple worker processes, but each individual worker processes
one job at a time.
Main component responsibilities
  Component                           Responsibility
  CLI                                 Accepts commands and displays job
information

  Task registry                       Maps task names to task functions
and input schemas

  Job service                         Implements database operations and
scheduling state transitions

  Database module                     Configures SQLAlchemy and creates
database sessions

  Models                              Defines the PostgreSQL tables
through SQLAlchemy

  Worker                              Claims jobs, launches task
subprocesses, monitors execution,
and handles recovery

  Task runner                         Entry point executed in the child
Python process

  Executor                            Validates serialized task input and
invokes the registered function

  Migration script                    Applies the currently supported
manual schema additions
Initialization script               Creates tables for a fresh database
4. Job lifecycle
The intended normal lifecycle is:
                 submit
                   |
                   v
                QUEUED
                   |
             worker claims
                   |
                   v
                RUNNING
             /     |      \
            /      |       \
           v       v        v
      COMPLETED  FAILED  TIMED_OUT
There is also a cancellation path:
QUEUED  ---- cancel request ---->  CANCELLED
In the current code, cancellation is only allowed while a job is still
queued. Running-job cancellation is not implemented.
State meanings
- QUEUED: stored and waiting for a worker.
- RUNNING: claimed by a worker and being processed.
- COMPLETED: the task subprocess exited successfully and the worker
  recorded success.
- FAILED: task execution failed or the worker encountered an
  exception while processing it.
- TIMED_OUT: execution exceeded the configured limit and the worker
  terminated the task process group.
- CANCELLED: a queued job was cancelled before a worker claimed it.
The statuses are currently strings, not a database enum or constrained
type. Formal status constraints should be added later.
5. Source files: purpose and relationships
5.1 pyproject.toml --- package configuration
This file defines the Python package, build backend, dependencies,
supported Python version, and installed CLI command.

Important decisions:
- setuptools is used as the build backend.
- Python 3.12 or newer is required.
- SQLAlchemy provides the ORM and database abstraction.
- psycopg is the PostgreSQL driver.
- python-dotenv loads local environment variables.
- Pydantic validates task input.
- The console script maps the forkline command to
  forkline.cli:main.
Relationship: installation metadata makes the forkline command
available; the CLI then imports the application modules.
5.2 database.py --- database connection and sessions
This module:
1. Loads environment variables from .env.
2. Checks that DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and
   DB_NAME exist.
3. Builds a PostgreSQL SQLAlchemy URL using the postgresql+psycopg
   driver.
4. Creates the SQLAlchemy engine.
5. Creates SessionLocal.
6. Defines the declarative Base used by ORM models.
7. Provides get_db() to yield a session and close it afterward.
The actual credentials must remain in the local .env file and must
never be committed to Git.

Relationship: models.py uses Base; job_service.py, init_db.py,
and migrate_db.py use the engine or session factory.
5.3 models.py --- database schema
Two ORM models are defined.

Job
- id: integer primary key
- task_name: registered task name
- status: current lifecycle status, defaulting to QUEUED
- priority: integer, defaulting to zero
- payload: serialized task input
- created_at: database-generated creation timestamp
- started_at: timestamp when a worker claims the job
- worker_id: identifier of the worker that owns the running job
Worker
- id: worker identifier, currently constructed from hostname and
  process ID
- last_heartbeat: last recorded heartbeat timestamp
- status: worker status, such as ACTIVE or OFFLINE
Relationship: job_service.py queries and updates these models.
init_db.py uses their metadata to create tables.

Current schema limitation: Job.worker_id is not defined as a foreign
key to Worker.id. Status values also have no database check
constraint.
5.4 schemas.py --- input validation
WaitTaskInput is a Pydantic model with a seconds field constrained
to be greater than zero.

The task registry associates each task with an input schema. The CLI
uses that schema to validate user input before saving a job. The
executor validates the serialized payload again before calling the task
function.

The Job Pydantic schema is also present in the shared source, but it
is not currently central to the service or CLI flow shown.
5.5 tasks.py --- actual task implementations and registry
This module contains the functions that can be scheduled.

run_wait(seconds) simply sleeps for the requested duration. It is
a controlled test task for queueing, execution, and timeout behavior.

run_spawn_child(seconds) launches another Python child process
that sleeps, prints its PID, and then waits for the configured duration.
Its finally block terminates and waits for the child if it is still
running. This task is intended to help test process-tree cleanup.

TASK_REGISTRY maps a public task name to its function and Pydantic
input schema. The CLI and executor both depend on this registry.

Relationship: cli.py uses the registry to expose valid task names and
validate submissions. executor.py uses it to locate and execute the
function.
5.6 job_service.py --- database operations and scheduler state
This is the central database/service layer. It contains operations for:
- Submitting a job
- Listing jobs
- Fetching one job
- Cancelling a queued job
- Claiming the next queued job
- Updating a job's terminal status
- Updating a worker heartbeat
- Marking a worker offline
- Detecting stale workers
- Recovering jobs owned by offline workers
Job submission
submit_job() serializes the validated Pydantic input with
model_dump_json(), creates a Job, commits it, refreshes it, and
returns its ID.
Claiming a job
claim_next_job(worker_id) selects the oldest queued job, applies
with_for_update(skip_locked=True), changes the job to RUNNING,
records started_at and worker_id, commits, and returns the job ID.

This transaction is the concurrency-control point. The lock prevents two
cooperating workers from successfully claiming the same row at the same
time.
Ownership-aware status updates
update_job_status() only updates a job if its ID matches, its current
status is RUNNING, and its worker_id matches the worker attempting
the update. This prevents a worker that no longer owns a job from freely
overwriting its state.
Heartbeats and recovery
Workers periodically update their heartbeat.
mark_stale_workers_offline() marks active workers offline when their
heartbeat is older than the configured threshold.
recover_jobs_from_offline_workers() then looks for running jobs
associated with offline workers and conditionally changes them back to
QUEUED, clearing their worker and start-time fields.

The conditional update is important because the job may have changed
since it was first selected. The update rechecks the job state and
expected owner.

Important limitation: a stale heartbeat is evidence of lost contact, not
proof that the worker process has died. A recovered task may overlap
with an old task that is still running.
5.7 worker.py --- worker process and execution supervisor
This module runs the worker loop.

Its responsibilities include:
1. Construct a worker ID from hostname and process ID.
2. Install signal handlers for SIGINT and SIGTERM.
3. Attempt job recovery on startup.
4. Start a heartbeat thread.
5. Periodically run recovery checks.
6. Claim a queued job.
7. Fetch the job payload.
8. Launch forkline.task_runner as a separate Python process.
9. Wait for the task with a 10-second timeout.
10. On timeout, send SIGTERM to the task process group, wait three
    seconds, and then send SIGKILL if necessary.
11. Record COMPLETED, FAILED, or TIMED_OUT.
12. On shutdown, stop the heartbeat loop, mark the worker offline, and
    exit.
The child process is started with start_new_session=True. This gives
the task runner its own process group/session so the worker can signal
the task process group during timeout handling.

The worker currently runs one job at a time. Running multiple worker
processes provides parallel execution across workers.
5.8 task_runner.py --- child-process entry point
This small module reads the task name and JSON payload from command-line
arguments and passes them to execute_task().

It is deliberately separate from the worker. The worker supervises a
process; the task runner performs the task.

A task exception that escapes the runner causes a non-zero process exit.
The worker checks the exit code and records failure.
5.9 executor.py --- payload validation and function invocation
execute_task(task_name, payload):
1. Looks up the task in TASK_REGISTRY.
2. Gets the associated Pydantic schema and function.
3. Parses and validates the JSON payload with model_validate_json().
4. Converts the validated model to a dictionary with model_dump().
5. Calls the task function using keyword arguments.
This creates a controlled boundary between serialized database data and
Python function calls.
5.10 init_db.py --- initial database setup
This script calls Base.metadata.create_all() to create tables that do
not exist. It is useful for setting up a fresh development database.

It is not a replacement for a complete migration system when the schema
evolves.
5.11 migrate_db.py --- limited manual migration
The current migration script checks whether the jobs table has
started_at and worker_id columns. If missing, it adds them. It then
calls create_all() to create missing tables.

This is a small, project-specific migration helper. It does not provide
migration history, version tracking, rollback scripts, or the broader
capabilities of Alembic.
5.12 init.py --- currently empty
The file is present but contains no implementation in the supplied
snapshot. It currently has no active role in the architecture and can be
removed if it is not needed, or given a clearly defined purpose later.
6. Important design decisions and why they were made
  Decision                   Reason                  Current trade-off /
limitation
  PostgreSQL for job storage Durable shared state    Requires a running
across CLI and workers  database and connection
management

  SQLAlchemy ORM             Keeps database          ORM behavior and
operations organized    transaction boundaries
around models and       must be understood
sessions                carefully

  Pydantic task schemas      Validates user input    Each task needs a
and stored task         compatible schema and
payloads                registry entry

  Task registry              Makes tasks             Registry is currently
discoverable by name    in-process Python
without hardcoding      configuration
every task branch       

  CLI entry point            Provides a simple       No HTTP API or
operational interface   dashboard yet

  Separate worker process    Decouples execution     Worker lifecycle must
from submission         be supervised

  Separate task subprocess   Isolates task failures  Child processes and
and enables timeout     descendants require
supervision             robust cleanup

  FOR UPDATE SKIP LOCKED   Lets multiple workers   Correctness depends on
claim different jobs    a short, well-defined
without blocking on the transaction
same queued row         

  Worker ID ownership        Associates a running    Worker identity and
job with the worker     lease expiry need
that claimed it         stronger treatment for
production

  Heartbeat thread           Keeps worker liveness   Heartbeats can be
information fresh while delayed or lost even
the main thread         when a worker is alive
supervises tasks        

  Timeout with TERM then     Gives tasks a chance to Hard termination may
KILL                       stop cleanly before     interrupt task cleanup
force termination       

  Periodic recovery          Attempts to restore     Can cause duplicate
jobs left running by    execution if the old
offline workers         worker is merely
delayed
Manual migration script    Quickly supports the    Should be replaced by
current small schema    versioned Alembic
evolution               migrations
7. What has been implemented and manually exercised?
Based on the development history shared:
- Python package structure and installable forkline CLI
- PostgreSQL connection through SQLAlchemy
- Job and worker ORM models
- Database initialization
- Job submission and listing
- Fetching an individual job
- Queued-job cancellation path
- Task registry and Pydantic task validation
- wait task execution
- Worker loop and subprocess-based task execution
- Completion and failure status handling
- Timeout handling using process-group signals
- Graceful shutdown request handling
- Worker heartbeat updates
- Stale-worker detection
- Job recovery from offline workers
- Concurrent-safe job claiming using row locks and SKIP LOCKED
- Ownership-aware terminal status updates
- spawn_child task for testing child-process cleanup
- Manual tests involving successful jobs, failed jobs, timeouts,
  worker recovery, and rejection of a status update from the wrong
  worker
These were manual development checks, not a formal automated test suite.
There is currently no test directory or README according to the project
status reported.
8. Current limitations and issues to fix
These are important engineering findings from the source snapshot, not
reasons to abandon the design.
8.1 Correctness and reliability
1. Heartbeat recovery can duplicate work. A worker can be alive but
   delayed. Requeuing its job based only on an old heartbeat can allow
   the same task to run twice.
2. Worker hard-crash cleanup is incomplete. The worker's timeout
   handler can kill the task process group, but if the worker itself is
   forcibly killed, the task subprocess may survive because it is in a
   separate session.
3. Queued cancellation has a race. The current implementation reads
   a queued job and then changes its status. A worker could claim it
   between those operations. Replace this with one conditional database
   update that only changes QUEUED to CANCELLED.
4. No retry policy or attempt count. A failed job is marked failed;
   there is no configurable retry/backoff policy.
5. No result or error persistence. The system does not yet store
   structured task results, exception details, or captured output in
   dedicated columns.
6. No exactly-once guarantee. Even with ownership checks, an
   external side effect performed by a task cannot automatically be
   undone if a worker loses ownership or crashes. The honest goal is
   controlled at-least-once execution with idempotent tasks, unless
   stronger coordination is added.
8.2 Scheduling and schema
- priority exists but is not used in claim ordering; current
  scheduling is FIFO by job ID.
- Job.worker_id has no foreign-key constraint.
- Status strings are not constrained by a database enum/check
  constraint.
- There is no attempts counter, scheduled execution time, lease token,
  or result field yet.
- The current worker handles one task at a time.
- There is no configurable worker concurrency limit beyond starting
  more worker processes.
8.3 Code quality findings to verify/fix
- In the supplied cli.py, the annotation ArgumentParser.Namespace
  appears to reference an undefined name. It should be
  argparse.Namespace (or use a postponed annotation).
- handle_get_job() calls SystemExit(1) without raise; this does
  not exit at that point. Use raise SystemExit(1).
- executor.py has a misspelled "Unknown" error and reports the
  missing task definition rather than the requested task name.
- job_service.py contains duplicate/unused imports and a redundant
  explicit close() in a finally block around a context-managed
  session.
- task_runner.py should validate the number of command-line
  arguments and produce a clear error for malformed invocation.
- The pasted database.py began with a standalone backslash. If that
  character is literally in the file rather than a copy/paste
  artifact, remove it because it is invalid Python syntax.
- The current CLI submit handler constructs the selected schema using
  a seconds argument. That works for the current registered tasks,
  but it will not scale to tasks with different input fields. A
  generic CLI input format or task-specific argument parser will be
  needed.
9. What we are going to fulfill for the complete project
This is the intended completion plan we discussed: evolve Forkline from
the current working scheduler prototype into a well-engineered,
demonstrable systems project. The list below is the target scope, not a
claim that it is already implemented.
Phase 1 --- Stabilize the current foundation
- Fix the CLI annotation and exit-code issues.
- Clean duplicate and unused imports.
- Correct error messages and task-runner argument validation.
- Make cancellation an atomic conditional update.
- Verify fresh database initialization and schema migration behavior.
- Add a .gitignore that excludes .env, .venv, caches, and local
  artifacts.
- Add a README documenting setup, configuration, commands, and
  architecture.
Completion criterion: a new developer can clone the repository,
configure PostgreSQL, install Forkline, initialize the database, submit
a task, run a worker, and inspect the result using documented commands.
Phase 2 --- Formal automated testing
Create a tests/ suite using pytest.

Tests should cover:
- Pydantic input validation
- Job submission and serialization
- Listing and fetching jobs
- Cancellation of queued jobs
- Rejection of cancellation after a job is running
- Successful task execution
- Unknown task handling
- Task exceptions and failed exit codes
- Timeout and process-group termination
- Two workers attempting to claim jobs concurrently
- Worker ownership checks
- Heartbeat update and stale-worker detection
- Recovery of jobs from offline workers
- Recovery race conditions
- Child-process cleanup
Use a dedicated test database and isolate tests from development data.
Include integration tests for PostgreSQL-specific locking behavior;
SQLite alone cannot validate PostgreSQL SKIP LOCKED semantics.

Completion criterion: repeatable tests verify both normal paths and
failure/concurrency paths.
Phase 3 --- Stronger worker liveness and crash recovery
The current heartbeat-only design needs a stronger ownership mechanism.

Planned improvements:
- Add a lease/claim-expiry concept to jobs.
- Refresh the lease while the worker is alive.
- Reclaim a job only after its lease expires.
- Use an ownership token or monotonically changing attempt identifier
  so an old worker cannot update a newer attempt.
- Add an attempt counter and maximum retry count.
- Define retryable versus non-retryable failures.
- Add exponential backoff or a simpler configurable retry delay.
- Improve child-process cleanup when the worker is terminated
  unexpectedly.
- Test the case where a worker is paused or disconnected but not
  actually dead.
A lease reduces false recovery, but it still does not make arbitrary
external side effects exactly-once. Tasks that send messages, charge
payments, or modify external systems should be designed to be
idempotent.

Completion criterion: recovery behavior is explicit, tested, and
resistant to stale workers overwriting newer job attempts.
Phase 4 --- Job results, errors, and observability
Add persistent execution metadata:
- attempts
- finished_at
- result
- error_message or structured error information
- optionally execution duration and exit code
Add structured logging with job ID, worker ID, task name, attempt
number, and lifecycle event. Ensure logs make it possible to follow one
job across submission, claim, execution, failure, and recovery.

Completion criterion: a user can inspect not only the current status
but also why a job failed, how long it ran, and what result it produced.
Phase 5 --- Scheduling and queue behavior
Complete the scheduling behavior implied by the schema:
- Make priority meaningful in claim ordering.
- Define how equal-priority jobs are ordered (for example, oldest
  first).
- Consider delayed/scheduled jobs with a scheduled_at timestamp.
- Add a clear policy for cancelled, expired, retried, and dead-letter
  jobs.
- Add database indexes for common queue and recovery queries.
Completion criterion: queue ordering and delayed execution are
documented, implemented, and covered by tests.
Phase 6 --- Operational controls and production-oriented hardening
- Add configurable worker timeout, heartbeat interval, lease duration,
  recovery interval, and retry policy.
- Add a worker command with clear start/stop behavior.
- Improve graceful shutdown and ensure the worker does not claim new
  jobs after shutdown is requested.
- Handle database outages and reconnect behavior deliberately.
- Add structured health/status reporting.
- Add Alembic for versioned schema migrations.
- Add CI to run formatting, linting, type checks where appropriate,
  and tests.
- Add deployment documentation and a repeatable deployment setup.
Completion criterion: the project can be installed, migrated,
tested, run, and diagnosed using documented operational procedures.
Phase 7 --- Demonstration and project presentation
Prepare reproducible demonstrations for:
1. Submit a job and execute it.
2. Run multiple workers and show that they claim different jobs.
3. Submit a task that exceeds the timeout and show termination.
4. Submit a task that fails and inspect the failure record.
5. Stop or crash a worker and demonstrate recovery.
6. Attempt an invalid ownership update and show that it is rejected.
7. Demonstrate child-process cleanup.
8. Show logs and database state throughout the lifecycle.
Create architecture diagrams, a concise README, a technical design
document, and a short project presentation explaining the concurrency
and recovery decisions.

Completion criterion: another engineer can reproduce the key
behaviors and understand why the design uses PostgreSQL locking,
subprocess isolation, heartbeats, and recovery.
10. Suggested final repository structure
The following is a target organization. It is a proposal, not a claim
that all these files already exist.
forkline/
├── pyproject.toml
├── README.md
├── .gitignore
├── docs/
│   ├── architecture.md
│   └── design-decisions.md
├── src/
│   └── forkline/
│       ├── __init__.py
│       ├── cli.py
│       ├── database.py
│       ├── models.py
│       ├── schemas.py
│       ├── tasks.py
│       ├── executor.py
│       ├── task_runner.py
│       ├── worker.py
│       ├── job_service.py
│       └── migrations/
└── tests/
    ├── conftest.py
    ├── test_job_service.py
    ├── test_executor.py
    ├── test_worker.py
    ├── test_recovery.py
    └── test_concurrency.py
11. How to explain Forkline in a project review or interview
Short explanation
"Forkline is a persistent multiprocess job scheduler built with Python
and PostgreSQL. It separates job submission from execution, stores job
state durably, and uses independent workers to claim queued jobs.
Workers use PostgreSQL row locking with SKIP LOCKED to avoid claiming
the same job concurrently, and execute tasks in isolated subprocesses
with timeout handling. Worker heartbeats and recovery logic allow jobs
left running by offline workers to be reconsidered. The project focuses
on concurrency, process management, failure handling, and reliable state
transitions."
What makes it technically interesting?
The most important technical points to explain are:
1. Why a database-backed queue is used instead of an in-memory list.
2. How FOR UPDATE SKIP LOCKED prevents two workers from claiming the
   same queued row.
3. Why task execution happens in a child process rather than inside the
   worker.
4. How process groups are used to terminate a task and its child
   processes on timeout.
5. How heartbeats and recovery work, and why heartbeat-based recovery
   can still produce duplicate execution.
6. Why worker ownership checks matter when updating job state.
7. Why exactly-once execution is difficult when tasks have external
   side effects.
Be candid about current limitations. A strong engineering explanation
includes the failure cases that the design does not yet solve.
12. Immediate next steps
The most sensible order is:
1. Verify and fix the small source issues listed in Section 8.3.
2. Add the README and .gitignore.
3. Build automated tests around the behavior already implemented.
4. Strengthen recovery with leases and attempt ownership.
5. Add result/error persistence and structured logging.
6. Implement priority scheduling and any delayed-job behavior selected
   for the final scope.
7. Add Alembic, CI, deployment instructions, and reproducible
   demonstrations.
Do not jump straight to deployment before the concurrency and recovery
behavior is tested. The scheduler's reliability is its core engineering
claim, so those mechanisms should be verified first.
13. Current status in one sentence
Forkline already has a meaningful end-to-end foundation---persistent job
submission, database-backed worker claiming, subprocess execution,
timeouts, heartbeats, and recovery---but it still needs automated tests,
stronger lease-based crash recovery, result/error observability,
complete scheduling semantics, migration management, and project
documentation before it should be presented as production-ready.