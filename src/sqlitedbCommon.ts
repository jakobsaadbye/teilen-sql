import { flatten, sqlPlaceholdersNxM } from "./utils.ts";
import { CrrColumn } from "./change.ts";
import { defaultUpgradeOptions, SqliteColumnInfo, SqliteDB, SqliteForeignKey } from "./sqlitedb.ts";

export const upgradeTableToCrr = async (db: SqliteDB, tblName: string, options = defaultUpgradeOptions) => {
    // Mixin default options with user options
    options = { ...defaultUpgradeOptions, ...options };

    const columnInfos = await db.select<SqliteColumnInfo[]>(`PRAGMA table_info('${tblName}')`, []);
    if (columnInfos.length === 0) {
        console.error(`'${tblName}' is not a recognized table. Make sure it exists in the database before upgrading it to a crr`);
        return;
    }

    const fks = await db.select<SqliteForeignKey[]>(`PRAGMA foreign_key_list('${tblName}')`, []);

    const crrColumns: CrrColumn[] = [];
    for (const col of columnInfos) {
        const fkInfo = fkOrNull(col, fks);

        let fk: string | null = null;
        let fkOnDelete = null;
        if (fkInfo) {
            fk = `${fkInfo.table}|${fkInfo.to}`;
            fkOnDelete = fkInfo.on_delete;
        }

        const manualConflict = columnOptionValue(options.manualConflict!, col.name);
        const replicate = columnOptionValue(options.replicate!, col.name);

        const crrColumn: CrrColumn = {
            tbl_name: tblName,
            col_id: col.name,
            type: "lww",
            fk: fk,
            fk_on_delete: fkOnDelete,
            parent_col_id: null,
            manual_conflict: manualConflict ? 1 : 0,
            replicate: replicate ? 1 : 0,
        }

        crrColumns.push(crrColumn);
    }

    const columns = Object.keys(crrColumns[0]);
    const values = flatten(crrColumns.map(col => Object.values(col)));

    const err = await db.exec(`
            INSERT OR REPLACE INTO "crr_columns" (${columns.join(',')})
            VALUES ${sqlPlaceholdersNxM(columns.length, crrColumns.length)}
        `, values);
    if (err) return console.error(err);
}

const columnOptionValue = (option: "all" | "none" | { include?: string[], exclude?: string[] }, column: string) => {
    let value = false;
    if (option === "all") value = true;
    else if (option === "none") value = false;
    else {
        if (option.include) {
            if (option.include.includes(column)) value = true;
            else value = false;
        } else if (option.exclude) {
            if (option.exclude.includes(column)) value = false;
            else value = true;
        }
    }
    return value;
}

const fkOrNull = (col: any, fks: SqliteForeignKey[]): SqliteForeignKey | null => {
    const fk = fks.find(fk => fk.from === col.name);
    if (fk === undefined) return null;
    return fk
}