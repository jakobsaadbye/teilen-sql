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

    commit;
`;