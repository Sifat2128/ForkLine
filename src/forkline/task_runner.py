import sys 

from forkline.executor import execute_task

def main()->None:
    if len(sys.argv) != 3:
        print(
            "Usage: python -m forkline.task_runner <task_name> <payload_json>",
            file=sys.stderr,
        )
        raise SystemExit(2)

    task_name = sys.argv[1]
    payload = sys.argv[2]
    execute_task(task_name , payload)

if __name__ == "__main__":
    main()