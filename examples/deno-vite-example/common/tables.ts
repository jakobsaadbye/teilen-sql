export const tables = `
    CREATE TABLE IF NOT EXISTS "todos" (
        id text primary key,
        title text,
        finished bool,
        createdAt text
    );
`;