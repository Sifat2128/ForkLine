
import argparse
from pydantic import ValidationError
from forkline.job_service import submit_job,list_jobs,get_job,cancel_queued_job
# from forkline.schemas import WaitTaskInput
from forkline.tasks import TASK_REGISTRY


def handle_submit(args: argparse.Namespace) -> None:
    task_definition = TASK_REGISTRY.get(args.task)

    if task_definition is None:
        print(f"Unknown task: {args.task}")
        raise SystemExit(2)

    input_schema = task_definition["schema"]

    try:
        task_input = input_schema(seconds=args.seconds)
    except ValidationError as exc:
        for error in exc.errors():
            print(f"Validation error: {error['msg']}")
        raise SystemExit(2)

    job_id = submit_job(
        task_name=args.task,
        task_input=task_input,
    )
    print(f"Job submitted successfully. ID: {job_id}")
    print("Status: QUEUED")



def handle_list(args: argparse.Namespace) -> None:
    jobs = list_jobs(limit=args.limit)

    if not jobs:
        print("No jobs found.")
        return

    print(f"{'ID':<6} {'TASK':<12} {'STATUS':<12} {'PRIORITY':<9} {'CREATED AT'}")
    print("-" * 75)

    for job in jobs:
        print(
            f"{job.id:<6} "
            f"{job.task_name:<12} "
            f"{job.status:<12} "
            f"{job.priority:<9} "
            f"{job.created_at}"
        )
def handle_get_job(args:argparse.Namespace)->None:
    job = get_job(args.job_id)
    if not job:
        print(f"Job with id {args.job_id} does not exist")
        raise SystemExit(1)
    print(f"ID:         {job.id}")
    print(f"Task:       {job.task_name}")
    print(f"Status:     {job.status}")
    print(f"Priority:   {job.priority}")
    print(f"Payload:    {job.payload}")
    print(f"Created at: {job.created_at}")

def handle_cancel_queued_job(args:argparse.Namespace) -> None:
    cancelled = cancel_queued_job(args.job_id)
    if cancelled:
        print(f"Job {args.job_id} cancelled successfully.")
    else:
        print(
            f"Job {args.job_id} was not cancelled. "
            "It may not exist or may no longer be queued."
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="forkline",
        description="A persistent multiprocess job scheduler",
    )

    sub_commands = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Submit command
    submit_parser = sub_commands.add_parser(
        "submit",
        help="Submit a new job",
    )

    submit_parser.add_argument(
        "task",
        choices=list(TASK_REGISTRY.keys()),
        help="Approved task to execute",
    )

    submit_parser.add_argument(
        "--seconds",
        type=float,
        required=True,
        help="Number of seconds for the task",
    )

    submit_parser.set_defaults(handler=handle_submit)

    # List command
    list_parser = sub_commands.add_parser(
        "list",
        help="List submitted jobs",
    )

    list_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of jobs to display",
    )

    list_parser.set_defaults(handler=handle_list)

    # Show command
    get_job_parser = sub_commands.add_parser(
        "show",
        help="Show a job by ID",
    )

    get_job_parser.add_argument(
        "job_id",
        type=int,
        help="Job ID",
    )

    get_job_parser.set_defaults(handler=handle_get_job)

    # Cancel command
    cancel_job_parser = sub_commands.add_parser(
        "cancel",
        help="Cancel a queued job",
    )

    cancel_job_parser.add_argument(
        "job_id",
        type=int,
        help="ID of the job to cancel",
    )

    cancel_job_parser.set_defaults(handler=handle_cancel_queued_job)

    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)



if __name__ == "__main__":
    main()
