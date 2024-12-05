CREATE TABLE tasks (
    id SERIAL PRIMARY KEY, -- Автоинкремент для первичного ключа
    loaded_ts TIMESTAMP NOT NULL,
    task_name VARCHAR(256) NOT NULL,
    task_id UUID NOT NULL,
    task_creation_dt DATE NOT NULL,
    board_name VARCHAR(256) NOT NULL,
    column_name VARCHAR(256) NOT NULL,
    task_status VARCHAR(256),
    subtask_id UUID,
    subtask_name VARCHAR(256),
    subtask_status VARCHAR(256),
    quantity_plan NUMERIC,
    quantity_fact NUMERIC,
    delivery_term VARCHAR(256),
    loading_place VARCHAR(256),
    loading_start_date DATE,
    loading_end_date DATE,
    ship_name VARCHAR(256),
    discharging_place VARCHAR(256),
    deadline_start_date DATE,
    deadline_end_date DATE,
    prov_paid VARCHAR(64),
    final_paid VARCHAR(64)
);
