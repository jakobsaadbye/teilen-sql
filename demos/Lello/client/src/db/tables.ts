export const tables = `
    begin;

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
        position text
    );

    create table if not exists todos(
        id varchar(64) primary key,
        board_id varchar(64),
        column_id varchar(64) references columns(id) on delete cascade,
        title text,
        description varchar,
        position text,
        updated_at timestamptz default current_timestamp
    );

    create table if not exists crr_changes(
        row_id text not null,
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

    create table if not exists crr_columns(
        tbl_name text not null,
        col_id text not null,
        type text not null,
        fk text,
        fk_on_delete text,
        delete_wins_after bigint,
        parent_col_id text,
        primary key(tbl_name, col_id)
    );

    create table if not exists crr_clients(
        site_id primary key,
        last_pulled_at bigint not null default 0,
        last_pushed_at bigint not null default 0,
        is_me integer not null
    );

    commit;
`;

export const down = `
    begin;

    drop table if exists todos;
    drop table if exists columns;
    drop table if exists boards;
    drop table if exists crr_changes;

    commit;
`;
