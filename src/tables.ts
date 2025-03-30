export const insertCrrTablesStmt = `
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
        version text references crr_commits(id),

        primary key(type, tbl_name, col_id, pk, version)
    );

    CREATE index IF NOT EXISTS crr_changes_index ON crr_changes(tbl_name, created_at, version);

    CREATE TABLE IF NOT EXISTS crr_columns(
        tbl_name text not null,
        col_id text not null,
        type text not null,
        fk text,
        fk_on_delete text,
        parent_col_id text,
        primary key(tbl_name, col_id)
    );

    CREATE TABLE IF NOT EXISTS crr_clients(
        site_id primary key,
        last_pulled_at bigint not null default 0,
        last_pushed_at bigint not null default 0,
        last_pulled_commit references crr_commits(id),
        last_pushed_commit references crr_commits(id),
        head references crr_commits(id),
        is_me boolean not null,
        time_travelling boolean default 0
    );

    CREATE TABLE IF NOT EXISTS crr_hlc(
        lotr int primary key default 1,
        time bigint default 0
    );

    CREATE TABLE IF NOT EXISTS crr_commits(
        id text primary key,
        parent text,
        message text,
        author references crr_clients(site_id),
        created_at bigint
    );

    CREATE index IF NOT EXISTS crr_commits_index ON crr_commits(created_at);

    COMMIT;
` 