export const insertTablesStmt = `
    BEGIN;

    CREATE TABLE IF NOT EXISTS crr_changes(
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

    CREATE index IF NOT EXISTS crr_changes_index ON crr_changes(tbl_name);

    CREATE TABLE IF NOT EXISTS crr_columns(
        tbl_name text not null,
        col_id text not null,
        type text not null,
        fk text,
        fk_on_delete text,
        delete_wins_after bigint,
        parent_col_id text,
        primary key(tbl_name, col_id)
    );

    CREATE TABLE IF NOT EXISTS crr_clients(
        site_id primary key,
        last_pulled_at bigint not null default 0,
        last_pushed_at bigint not null default 0,
        is_me integer not null
    );

    COMMIT;
` 