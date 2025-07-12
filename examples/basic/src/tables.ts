export const commonTables = `
    CREATE TABLE IF NOT EXISTS "recipes" (
        id text primary key,
        title text,
        amount int default 0,
        prepTime int default 30
    );

    CREATE TABLE IF NOT EXISTS "users" (
        id text primary key,
        email text,
        firstName text,
        lastName text
    );
`