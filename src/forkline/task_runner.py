import sys 

from forkline.executor import execute_task

def main()->None:
    task_name = sys.argv[1]
    payload = sys.argv[2]
    execute_task(task_name , payload)

if __name__ == "__main__":
    main()