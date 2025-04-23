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
        document text references crr_documents(id),

        primary key(type, tbl_name, col_id, pk, version)
    );

    CREATE index IF NOT EXISTS crr_changes_index ON crr_changes(tbl_name, created_at, version, document);

    CREATE TABLE IF NOT EXISTS crr_columns(
        tbl_name text not null,
        col_id text not null,
        type text not null,
        fk text,
        fk_on_delete text,
        parent_col_id text,
        manual_conflict boolean,

        primary key(tbl_name, col_id)
    );

    CREATE TABLE IF NOT EXISTS crr_clients(
        site_id primary key,
        last_pulled_at bigint not null default 0,
        last_pushed_at bigint not null default 0,
        is_me boolean not null
    );

    CREATE TABLE IF NOT EXISTS crr_temp(
        lotr int primary key default 1,
        time bigint default 0,
        time_travelling boolean default 0,
        document text references crr_documents(id)
    );

    CREATE TABLE IF NOT EXISTS crr_commits(
        id text primary key,
        document text references crr_documents(id),
        parent text,
        message text,
        author references crr_clients(site_id),
        created_at bigint,
        applied_at bigint
    );

    CREATE index IF NOT EXISTS crr_commits_index ON crr_commits(created_at, document);

    CREATE TABLE IF NOT EXISTS crr_documents(
        id text primary key,
        head text references crr_commits(id),
        last_pulled_at bigint not null default 0,
        last_pulled_commit text references crr_commits(id),
        last_pushed_commit text references crr_commits(id)
    );

    CREATE TABLE IF NOT EXISTS crr_conflicts(
        document text references crr_documents(id),
        tbl_name text not null,
        pk text not null,
        columns string,
        base string,
        our string,
        their string,

        primary key(document, tbl_name, pk)
    );

    COMMIT;
` 