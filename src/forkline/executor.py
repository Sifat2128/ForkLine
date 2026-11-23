from . tasks import TASK_REGISTRY 

def execute_task(task_name : str , payload : str)-> None:
    task_definition = TASK_REGISTRY.get(task_name)
    if task_definition is None:
        raise ValueError(f"Unknow task : {task_name}")
    input_schema = task_definition["schema"]
    task_function = task_definition["function"]
    task_input = input_schema.model_validate_json(payload)
    task_function(**task_input.model_dump())