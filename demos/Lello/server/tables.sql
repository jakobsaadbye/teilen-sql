create table if not exists boards(
    id varchar(64) primary key,
    title varchar(255),
    created_at timestamptz default current_timestamp,
    updated_at timestamptz default current_timestamp
);

create table if not exists columns(
    id varchar(64) primary key,
    board_id varchar(64) references boards(id) on delete cascade,
    title varchar(255),
    position real
);

create table if not exists todos(
    id varchar(64) primary key,
    board_id varchar(64),
    column_id varchar(64) references columns(id) on delete cascade,
    title varchar(255),
    description varchar,
    position real,
    updated_at timestamptz default current_timestamp
);

create table if not exists crr_changes(
    type text not null,
    tbl_name text not null,
    col_id text,
    pk text not null,
    value any,
    site_id text not null,
    created_at bigint not null,
    applied_at bigint not null,
    primary key(type, tbl_name, col_id, pk)
);

create index if not exists crr_changes_index on crr_changes(tbl_name, created_at);

create table if not exists crr_columns(
    tbl_name text not null,
    col_id text not null,
    type text not null,
    fk text,
    fk_on_delete text,
    parent_col_id text,
    primary key(tbl_name, col_id)
);

create table if not exists crr_clients(
    site_id primary key,
    last_pulled_at bigint not null default 0,
    last_pushed_at bigint not null default 0,
    is_me integer not null
);