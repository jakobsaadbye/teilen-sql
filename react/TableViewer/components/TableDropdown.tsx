import { useEffect, useRef, useState } from "react";
import { RightClickTableEvent } from "@/react/TableViewer/types.ts";
import { useDB, useQuery } from "../../hooks.ts"
import { SqliteColumnInfo } from "@/sqlitedb.ts";

type TDProps = {
    tables: { name: string }[]
    event: RightClickTableEvent
}

export const TableDropdown = ({ tables, event }: TDProps) => {
    const db = useDB();

    const menuRef = useRef<HTMLElement | null>(null);

    const exportSql = async () => {
        if (event === undefined) return

        const tblName = tables[event.tableIndex].name;

        const cols = await db.select<SqliteColumnInfo[]>(`PRAGMA table_info('${tblName}')`, []);
        const rows = await db.select<any[]>(`SELECT * FROM "${tblName}"`, []);

        const values = rows.map(r => Object.values(r)).map(vals => `(${vals.map(v => `'${v}'`).join(',')})`).join(',\n');
        const sql = `
            INSERT INTO "${tblName}" (${cols.map(c => c.name).join(', ')})
            VALUES ${values};
        `;
        const link = document.createElement("a");
        const file = new Blob([sql], { type: 'text/plain' });
        link.href = URL.createObjectURL(file);
        link.download = `${tblName}.sql`;
        link.click();
        URL.revokeObjectURL(link.href);
    }

    const exportJson = async () => {
        if (event === undefined) return

        const tblName = tables[event.tableIndex].name;

        const rows = await db.select(`SELECT * FROM "${tblName}"`, []);
        const json = JSON.stringify(rows);

        const link = document.createElement("a");
        const file = new Blob([json], { type: 'application/json' });
        link.href = URL.createObjectURL(file);
        link.download = `${tblName}.json`;
        link.click();
        URL.revokeObjectURL(link.href);
    }

    const items = [
        { name: "Export SQL", onClick: exportSql },
        { name: "Export JSON", onClick: exportJson }
    ];

    useEffect(() => {
        if (menuRef.current === null) return
        if (event === undefined) return

        menuRef.current.style.top = `${event.mouseY}px`
        menuRef.current.style.left = `${event.mouseX}px`
    }, [event])

    if (!event) return <></>
    return (
        <div ref={menuRef} className={`absolute z-50 p-1 bg-gray-200 border border-gray-400 w-32`}>
            <div className="flex flex-col space-y-2">
                {items.map((item, i) => (
                    <button key={i} onClick={item.onClick} className="pl-1 text-start w-full border-b-gray-400 cursor-default hover:bg-gray-100">{item.name}</button>
                ))}
            </div>
        </div>
    )
}