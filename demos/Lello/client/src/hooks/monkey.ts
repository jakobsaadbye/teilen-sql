import { useState } from "react";
import { Syncer, SqliteDB } from "@teilen-sql";
import { useDB, useSyncer } from "@teilen-sql-react"
import { BoardRepo } from "../db/repo/boardRepo.ts";
import { Column, Todo } from "../db/types.ts";

const columnTitles = ["Procrastination Station", "Ideas That Seemed Good at 2 AM", "To-Don't List", "Oops, Wrong Column", "Future Million-Dollar Ideas", "Oops, Already Done", "Tasks Nobody Wants", "Legendary Coffee Breaks", "Meetings That Could Be Emails", "When Pigs Fly", "Low-Key Panic Zone", "Epic Fails Archive", "Unicorn Hunting Grounds", "Infinite To-Dos", "Someday, Maybe... Probably Not", "Brainstorm or Brainfart?", "Overthinking Central", "Ctrl+Z All Day", "In Case of Emergency, Delete This", "High Hopes, Low Effort"];
const todoTitles = ["Buy Milk... and Cookies (for science)", "Figure Out Why Plants Keep Dying", "Finally Learn to Fold a Fitted Sheet", "Write 'TODO' List for Tomorrow", "Find Out Where All the Socks Go", "Become a Morning Person (Optional)", "Cancel Free Trial Before Getting Charged", "Invent Time Travel – Should Be Easy", "Train Cat to Fetch Coffee", "Schedule Nap Between Naps", "Finish Book I Started 3 Years Ago", "Rename Wi-Fi to 'Hack Me If You Can'", "Replace Batteries in Remote (Maybe)", "Find a New Hobby – Netflix Doesn't Count", "Apologize to Houseplants for Neglect", "Plan World Domination (Later, Too Busy)", "Teach Dog to Use Zoom", "Convince Friends I'm Funny", "Remember Why I Walked into This Room", "Prove to Myself I Can Cook (Good Luck)"];

type Action =
    | "pullChanges"
    | "pushChanges"
    | "addTodo"
    | "addColumn"
    | "removeTodo"
    | "removeColumn"
    | "moveTodo"
    | "moveColumn"
    | "updateTodoTitle"
    | "updateColumnTitle"

const rngActions = [
    ["pullChanges", 0.40],
    ["pushChanges", 0.40],
    ["addTodo", 0.80],
    ["addColumn", 0.40],
    ["removeTodo", 0.60],
    ["removeColumn", 0.10],
    ["moveTodo", 0.70],
    ["moveColumn", 0.70],
    ["updateTodoTitle", 1.0],
    ["updateColumnTitle", 1.0]
] as [action: Action, prop: number][];

let stop = false;

export const useGoBananas = (boardId?: string) => {
    const db = useDB();
    if (db === null) {
        throw new Error(`Failed to retreive db from context in useBananas(). Make sure the component this hook is used in, is inside of a SqliteContext.Provider`)
    }

    const syncer = useSyncer("http://127.0.0.1:3000/changes");

    const cancel = () => { stop = true; setRunning(false); }

    const [running, setRunning] = useState(false);

    const goBananas = async (duration = 5_000) => {
        if (boardId === undefined) return;

        await clearBoard(db, boardId);

        setRunning(true);
        stop = false;
        const doAction = async () => {
            if (stop) return;

            const action = pickRandomAction(rngActions);
            
            switch (action) {
                case 'pullChanges': { await pullChanges(syncer); break; }
                case 'pushChanges': { await pushChanges(syncer); break; }
                case 'addColumn': { await addColumn(db, boardId); break; }
                case 'addTodo': { await addTodo(db, boardId); break; }
                case 'removeTodo': { await removeTodo(db, boardId); break; }
                case 'removeColumn': { await removeColumn(db, boardId); break; }
                case 'moveTodo': { await moveTodo(db, boardId); break; }
                default: break;
            }

            if (!stop) {
                setTimeout(doAction, 10);
            }
        };

        doAction();

        setTimeout(cancel, duration);
    }

    return [goBananas, cancel, running];
}

const clearBoard = async (db: SqliteDB, boardId: string) => {
    await db.exec(`DELETE FROM "columns" WHERE board_id = ?`, [boardId]);
}

const pullChanges = async (syncer: Syncer) => {
    await syncer.pullChanges();
}

const pushChanges = async (syncer: Syncer) => {
    await syncer.pushChanges();
}

const moveTodo = async (db: SqliteDB, boardId: string) => {
    const columns = await db.select<Column[]>(`SELECT * FROM "columns" WHERE board_id = ?`, [boardId]);
    if (columns.length === 0) return;

    const todos = await db.select<Todo[]>(`SELECT * FROM "todos" WHERE board_id = ?`, [boardId]);
    if (todos.length === 0) return;

    const randomCol = pickRandom(columns);
    const randomTodo = pickRandom(todos);

    const prependOrAppend = pickRandom(["-1", "1"]);
    await BoardRepo.saveTodo(db, { ...randomTodo, column_id: randomCol.id, position: prependOrAppend });
}

const addColumn = async (db: SqliteDB, boardId: string) => {
    const column: Column = {
        id: crypto.randomUUID(),
        board_id: boardId,
        title: pickRandom(columnTitles),
        position: "1"
    };

    await BoardRepo.saveColumn(db, column);
}

const addTodo = async (db: SqliteDB, boardId: string) => {
    const columns = await db.select<Column[]>(`SELECT * FROM "columns" WHERE board_id = ?`, [boardId]);
    if (columns.length === 0) return;

    const randomCol = pickRandom(columns);
    const todo: Todo = {
        id: crypto.randomUUID(),
        board_id: boardId,
        column_id: randomCol.id,
        title: pickRandom(todoTitles),
        description: "",
        position: "1",
        updated_at: (new Date).toISOString()
    }

    await BoardRepo.saveTodo(db, todo);
}

const removeTodo = async (db: SqliteDB, boardId: string) => {
    const rows = await db.select<{ id: string }[]>(`SELECT id FROM "todos" WHERE board_id = ?`, [boardId]);
    if (rows.length === 0) return;
    const todoIds = rows.map(row => row.id);
    const randomId = pickRandom(todoIds);

    await BoardRepo.deleteTodo(db, randomId);
}

const removeColumn = async (db: SqliteDB, boardId: string) => {
    const rows = await db.select<{ id: string }[]>(`SELECT id FROM "columns" WHERE board_id = ?`, [boardId]);
    if (rows.length === 0) return;
    const columnIds = rows.map(row => row.id);
    const randomId = pickRandom(columnIds);

    await BoardRepo.deleteColumn(db, randomId);
}

const pickRandomAction = (actions: [action: Action, p: number][]): Action => {
    const r = Math.random();
    while (true) {
        const [action, p] = pickRandom(actions);
        if (p > r) return action;
    }
}

const pickRandom = <T>(arr: T[]) => {
    const index = randIntBetween(0, arr.length - 1);
    return arr[index];
}

const randIntBetween = (a: number, b: number) => {
    const t = Math.random();
    return Math.round(a + t * (b - a));
}