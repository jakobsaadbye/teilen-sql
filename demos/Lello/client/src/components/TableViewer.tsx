import React, { ChangeEvent, KeyboardEvent } from "react";
import { useEffect, useRef, useState } from "react";
import { useIcon } from "../hooks/useIcon.ts";
import { useDB, useQuery } from "@teilen-sql/react.ts"
import { twMerge } from "tailwind-merge";
import { sqlDetermineOperation } from "@teilen-sql/change.ts";

type Props = {

}

type SelectedItems = {
    type: 'table' | 'row'
    items: number[]
} | undefined

type RightClickTableEvent = {
    tableIndex: number
    mouseX: number
    mouseY: number
} | undefined

export const TableViewer = ({ }: Props) => {
    const db = useDB();

    const [show, setShow] = useState(false);
    const [mode, setMode] = useState<'data' | 'structure' | 'query'>('data');
    const [fullscreen, setFullscreen] = useState(localStorage.getItem("tw_fullscreen") ?? "");
    const [selectedItems, setSelectedItems] = useState<SelectedItems>(undefined);
    const [tableRightClicked, setTableRightClicked] = useState<RightClickTableEvent>(undefined);
    const [st, setSt] = useState<string | undefined>(undefined);
    const [orderBy, setOrderBy] = useState({});

    const [sqlEditorOpen, setSqlEditorOpen] = useState(false);
    const [sqlEditorResults, setSqlEditorResults] = useState<{columns: any[], rows: any[]} | undefined>(undefined);
    const [sqlEditorError, setSqlEditorError] = useState<any | undefined>(undefined);

    const orderByString = (orderBy) => {
        if (orderBy[st] === undefined) return '';
        return `ORDER BY ${orderBy[st].columnName} ${orderBy[st].direction}`;
    }

    const { data: tables, isLoading: loadingTables } = useQuery<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, [], { once: false });
    const { data: rows } = useQuery<any[]>(`SELECT rowid, * FROM "${st}" ${orderByString(orderBy)} LIMIT 300`, [], { fireIf: st !== undefined, dependencies: [] });
    const { data: rowCount } = useQuery<{ count: number }>(`SELECT COUNT(*) AS count FROM "${st}"`, [], { fireIf: st !== undefined, first: true, dependencies: [] });
    const { data: columns } = useQuery(`PRAGMA table_info('${st}')`, [], { fireIf: st !== undefined });

    useEffect(() => {
        if (!loadingTables) {
            setSt(tables[0].name);
        }
    }, [loadingTables]);

    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            let handled = false;
            if (e.ctrlKey && e.key === 'd') {
                setShow(prev => !prev);
                handled = true;
            }
            if (selectedItems && e.key === 'Backspace') {
                handleDeleteItems();
                handled = true;
            }
            if (e.key === 'Escape') {
                deselectAll();
                handled = true;
            }
            if (e.metaKey && e.key === 'a') {
                selectAll();
                handled = true;
            }
            if (e.ctrlKey && e.key === 's') {
                setSqlEditorOpen(prev => !prev);
                handled = true;
            }
            if (e.ctrlKey && e.key === 'f') {
                if (!fullscreen) {
                    setFullscreen("Y");
                    localStorage.setItem("tw_fullscreen", "Y");
                } else {
                    setFullscreen("");
                    localStorage.setItem("tw_fullscreen", "");
                }
                handled = true;
            }

            if (handled) {
                e.preventDefault();
                e.stopPropagation();
            }

        };
        document.addEventListener('keydown', handleKeyDown);

        return () => {
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [selectedItems, fullscreen]);

    const onSqlEditorResults = (rows: any[]) : void => {
        if (rows.length === 0) return;
        const columns = Object.keys(rows[0]);
        setSqlEditorResults({ columns, rows });
        setMode('query');
    }

    const handleDeleteItems = async () => {
        if (selectedItems === undefined) return;
        if (selectedItems.type === 'table') {
            for (const i of selectedItems.items) {
                const err = await db.exec(`DROP TABLE IF EXISTS ${tables[i].name}`, []);
                if (err) console.error(err);
            }
        }
        if (selectedItems.type === 'row') {
            const rowIds = rows.filter((_, i) => selectedItems.items.includes(i)).map(r => r.rowid);
            const err = await db.exec(`DELETE FROM "${st}" WHERE rowid IN (${rowIds.map(id => `'${id}'`).join(',')})`, []);
            if (err) console.error(err);
        }

        deselectAll();
    }

    const setOrderByHelper = (value: any) => {
        setOrderBy({ ...orderBy, [st as string]: value })
    }

    const orderByColumn = (columnName: string) => {
        if (orderBy[st] === undefined) {
            setOrderByHelper({ columnName, direction: 'ASC' });
            return;
        }
        if (orderBy[st].columnName === columnName && orderBy[st].direction === 'DESC') { // Triple-click. Reset
            setOrderByHelper(undefined);
            return;
        }
        if (orderBy[st].columnName === columnName) { // Double-click
            setOrderByHelper({ columnName, direction: 'DESC' });
            return;
        }
        else { // Switch
            setOrderByHelper({ columnName, direction: 'ASC' });
        }
    }

    const selectAll = () => {
        setSelectedItems({ type: 'row', items: rows.map((_, i) => i) });
    }

    const deselectAll = () => {
        setSelectedItems(undefined);
        setTableRightClicked(undefined);
    }

    const isSelected = (type: 'table' | 'row', index: string) => {
        if (selectedItems === undefined) return false;
        if (selectedItems.type !== type) return false;

        return selectedItems.items.findIndex(idx => idx === index) !== -1;
    }

    const handleClickTable = (e: PointerEvent, tableIndex: number) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === "click") {
            setSt(tables[tableIndex].name);
            setSelectedItems({ type: 'table', items: [tableIndex] });
            if (mode === 'query') {
                setMode('data');
            }
        }
        if (e.type === "contextmenu") {
            setTableRightClicked({ tableIndex, mouseX: Math.floor(e.clientX), mouseY: Math.floor(e.clientY) });
        }
    }

    const handleClickRow = (e: PointerEvent, rowIndex: number) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === "click") {
            if (e.shiftKey) {
                if (selectedItems === undefined || selectedItems.type !== 'row') {
                    setSelectedItems({ type: 'row', items: [rowIndex] });
                } else {
                    const anchorA = selectedItems.items[0]
                    const anchorB = rowIndex;
                    const between = [];
                    if (anchorA > anchorB) for (let n = anchorA; n > anchorB; n--) between.push(n);
                    else for (let n = anchorA; n < anchorB; n++) between.push(n);

                    setSelectedItems({ type: 'row', items: [anchorA, ...between, anchorB] });
                }
            } else {
                setSelectedItems({ type: 'row', items: [rowIndex] });
            }
        }
    }

    const rowValues = (row) => {
        const { rowid, ...rest } = row;
        return Object.values(rest);
    }

    const { XIcon, Table, ChevronUp, ChevronDown } = useIcon();

    const height = fullscreen ? "h-full" : "h-96"

    if (!show) return <></>
    return (
        <>
            <TableDropdown tables={tables} event={tableRightClicked} />
            <div className={`absolute bottom-0 w-full ${height} rounded-md bg-white cursor-default overflow-clip`} onClick={deselectAll}>
                <header className="flex justify-end bg-gray-200">
                    <XIcon onClick={() => setShow(false)} className="w-8 h-8 fill-gray-500" />
                </header>
                <div className="relative flex h-full w-full">
                    <section className="bg-gray-300 p-2">
                        <h2 className="text-lg">Tables</h2>
                        <ul className="p-2">
                            {tables.map((table, i) => {
                                return (
                                    <div
                                        key={i}
                                        onClick={(e) => handleClickTable(e, i)}
                                        onContextMenu={(e) => handleClickTable(e, i)}
                                        className={`flex space-x-1 px-2 hover:bg-gray-200 ${isSelected('table', i) && 'bg-gray-100'}`}
                                    >
                                        <Table className="w-6 h-6 fill-blue-400" />
                                        <p className="select-none">{table.name}</p>
                                    </div>
                                )
                            })}
                        </ul>
                    </section>
                    <div className="h-full w-full overflow-y-auto bg-white">
                        <table className="flex-1 w-full bg-white mb-32">
                            <thead className="flex-1 bg-gray-200 sticky top-0 w-full">
                                <tr className="w-full">
                                    {mode === 'data' && columns && (columns.map((c, i) => (
                                        <th onClick={() => orderByColumn(c.name)} key={i} className="border-r border-gray-50">
                                            <div className="flex justify-center items-center">
                                                <p className="text-center">{c.name}</p>
                                                {orderBy[st]?.columnName === c.name && orderBy[st]?.direction === 'ASC' && <ChevronUp className="w-4 h-4 fill-gray-600" />}
                                                {orderBy[st]?.columnName === c.name && orderBy[st]?.direction === 'DESC' && <ChevronDown className="w-4 h-4 fill-gray-600" />}
                                            </div>
                                        </th>
                                    )))}
                                    {mode === 'structure' && columns && Object.keys(columns[0]).map((name, i) => (
                                        <th key={i} className="border-r border-gray-50">
                                            {name}
                                        </th>
                                    ))}
                                    {mode === 'query' && sqlEditorResults && sqlEditorResults.columns.map((name, i) => (
                                        <th key={i} className="border-r border-gray-50">
                                            {name}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="select-none">
                                {mode === 'data' && rows && rows.map((row, i) => (
                                    <tr
                                        onClick={(e) => handleClickRow(e, i)}
                                        key={i}
                                        className={twMerge(`flex-1 h-8 truncate overflow-scroll ${i % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'} ${isSelected('row', i) && 'bg-blue-400 text-white'}`)}
                                    >
                                        {rowValues(row).map((v: any, i) => <td key={i} className="border-r border-gray-50">{v}</td>)}
                                    </tr>
                                ))}
                                {mode === 'structure' && columns && columns.map((col, i) => (
                                    <tr key={i} className={`flex-1 h-8 truncate overflow-scroll ${i % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'}`}>
                                        {Object.values(col).map((v: any, i) => <td key={i} className="border-r border-gray-50">{v}</td>)}
                                    </tr>
                                ))}
                                {mode === 'query' && sqlEditorResults && sqlEditorResults.rows.map((row, i) => (
                                    <tr key={i} className={`flex-1 h-8 truncate overflow-scroll ${i % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'}`}>
                                        {Object.values(row).map((v: any, i) => <td key={i} className="border-r border-gray-50">{v}</td>)}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        <footer className="absolute bottom-8 py-2 mb-0 bg-gray-300 w-full">
                            <div className="flex justify-between">
                                <div className="flex space-x-2">
                                    <p onClick={() => setMode('data')} className={`px-4 ${mode === 'data' && 'bg-gray-100'}`}>Data</p>
                                    <p onClick={() => setMode('structure')} className={`px-4 ${mode === 'structure' && 'bg-gray-100'}`}>Structure</p>
                                </div>

                                {(mode === 'data' || mode === 'structure') && <p className="text-gray-600">{rowCount?.count ?? 0} rows</p>}
                                {mode === 'query' && <p className="text-gray-600">{sqlEditorResults?.rows.length ?? 0} rows</p>}
                                
                                <p></p>
                                <p></p>
                            </div>
                        </footer>
                    </div>

                    <SqlEditor isOpen={sqlEditorOpen} onResults={onSqlEditorResults} />
                </div>
            </div>
        </>
    );
}

type SqlEditorProps = {
    isOpen: boolean
    onResults: (rows: any[]) => void
}

const SqlEditor = ({ isOpen, onResults } : SqlEditorProps) => {
    const db = useDB();

    const [sql, setSql] = useState(localStorage.getItem("tw_sql_editor_query") ?? "");
    const [sqlError, setSqlError] = useState(undefined);
    const [lineCount, setLineCount] = useState(1);

    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            let handled = false;
            if (e.metaKey && e.key === 'Enter') {
                runSql(sql);
                handled = true;
            }

            if (handled) {
                e.preventDefault();
                e.stopPropagation();
            }

        };
        document.addEventListener('keydown', handleKeyDown);

        return () => {
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [sql]);

    const runSql = async (sql: string) => {
        console.log(sql);
        
        const operation = sqlDetermineOperation(sql);
        if (operation === 'select' || operation === 'pragma') {
            const {data, error} = await db.selectWithError(sql, []);
            if (error) {
                console.log(error.message);
                setSqlError(error.message);
            } else {
                onResults(data as any[]);
            }
        } else {
            const err = await db.exec(sql, []);
            if (err) {
                setSqlError(err.message);
            };
        }
    }

    const textChanged = (e: ChangeEvent) => {
        const numLines = e.target.value.split("\n").length;
        setLineCount(numLines);
        setSql(e.target.value);
        setSqlError(undefined);
        localStorage.setItem("tw_sql_editor_query", e.target.value);
    }

    const putCursorAtEndOfInput = (e: ChangeEvent) => {
        if (e === undefined) return;
        const t = e.target.value;
        e.target.value = '';
        e.target.value = t;
    }

    if (!isOpen) return <></>

    return (
        <div className="w-full p-1 border-l-4 border-gray-300">
            {sqlError && (
                <div className="">
                    <p className="text-red-400">{sqlError}</p>
                </div>
            )}
            <div className="flex w-full h-full space-x-2">
                <div className="flex flex-col">
                    {Array.from({length: lineCount}).map((_, i) => (
                        <p key={i} className="w-4 text-gray-400 select-none">{i+1}</p>
                    ))}
                </div>
                <textarea
                    className="w-full h-full font-normal focus:outline-none"
                    value={sql}
                    onChange={textChanged}
                    onFocus={putCursorAtEndOfInput}
                    autoFocus
                />
            </div>
        </div>
    )
}

type TDProps = {
    tables: { name: string }[]
    event: RightClickTableEvent
}

const TableDropdown = ({ tables, event }: TDProps) => {
    const db = useDB();

    const menuRef = useRef<HTMLElement | null>(null);

    const exportSql = async () => {
        if (event === undefined) return

        const tblName = tables[event.tableIndex].name;

        const cols = await db.select(`PRAGMA table_info('${tblName}')`, []);
        const rows = await db.select(`SELECT * FROM "${tblName}"`, []);

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
