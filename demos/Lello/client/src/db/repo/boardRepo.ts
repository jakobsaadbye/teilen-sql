import { SqliteDB } from "../../../../teilen-sql/sqlitedb.ts";
import { Board, BoardDetail, Column, Todo } from "../types.ts";

export class BoardRepo {
    static getBoard = async (db: SqliteDB, id: string): Promise<BoardDetail | undefined> => {
        const board = await db.first<Board>(`SELECT * FROM "boards" WHERE id=?`, [id]);
        if (board === undefined) return;
        const columns = await db.select<Column[]>(`SELECT * FROM "columns" WHERE board_id=? ORDER BY position ASC`, [id]);
        const todos = await db.select<Todo[]>(`SELECT * FROM "todos" WHERE board_id=? ORDER BY position ASC`, [id]);

        for (let i = 0; i < columns.length; i++) {
            columns[i].todos = todos.filter(t => t.column_id === columns[i].id);
        }

        return {
            ...board,
            columns: columns
        }
    }

    static saveBoard = async (db: SqliteDB, board: Board) => {
        const err = await db.execTrackChanges(`
            INSERT INTO "boards" (id, title, created_at, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = EXCLUDED.updated_at
        `, [board.id, board.title, board.created_at, board.updated_at]);
        if (err) console.error(err);
    }

    static deleteBoard = async (db: SqliteDB, id: string) => {
        const err = await db.execTrackChanges(`DELETE FROM "boards" WHERE id = $1`, [id]);
        if (err) console.error(err);
    }

    static saveColumn = async (db: SqliteDB, column: Column) => {
        const err = await db.execTrackChanges(`
            INSERT INTO "columns" (id, board_id, title, position)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                position = EXCLUDED.position
        `, [column.id, column.board_id, column.title, column.position]);
        if (err) console.error(err);
    }

    static deleteColumn = async (db: SqliteDB, id: string) => {
        const err = await db.execTrackChanges(`DELETE FROM "columns" WHERE id = $1`, [id]);
        if (err) console.error(err);
    }

    static saveTodo = async (db: SqliteDB, todo: Todo) => {
        const err = await db.execTrackChanges(`
            INSERT INTO "todos" (id, board_id, column_id, title, description, position, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                column_id 	= EXCLUDED.column_id,
                title 		= EXCLUDED.title,
                description = EXCLUDED.description,
                position    = EXCLUDED.position,
                updated_at 	= EXCLUDED.updated_at
        `, [todo.id, todo.board_id, todo.column_id, todo.title, todo.description, todo.position, todo.updated_at]);
        if (err) console.error(err);
    }

    static deleteTodo = async (db: SqliteDB, id: string) => {
        const err = await db.execTrackChanges(`DELETE FROM "todos" WHERE id = $1`, [id]);
        if (err) console.error(err);
    }
}