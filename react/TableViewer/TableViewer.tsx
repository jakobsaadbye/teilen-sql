import type { ChangeEvent, KeyboardEvent, PointerEvent } from "react";
import { useEffect, useRef, useState } from "react";
import { twMerge } from "tailwind-merge";
import { useDB, useQuery } from "../hooks.ts"
import { sqlDetermineOperation } from "../../change.ts";
import type { SqliteColumnInfo } from "@/sqlitedb.ts";

import XIcon from "./icons/X.tsx";
import TableIcon from "./icons/Table.tsx";
import ChevronUp from "./icons/ChevronUp.tsx";
import ChevronDown from "./icons/ChevronDown.tsx";
import OpenFullscreen from "./icons/OpenFullscreen.tsx";
import CloseFullscreen from "./icons/CloseFullscreen.tsx";

type SelectedItems = {
    type: 'table' | 'row'
    items: number[]
} | undefined

type RightClickTableEvent = {
    tableIndex: number
    mouseX: number
    mouseY: number
} | undefined

export const TableViewer = () => {
    const db = useDB();

    const [show, setShow] = useState(false);
    const [focused, setFocused] = useState(true);
    const [resultTableFocused, setResultTableFocused] = useState(true);
    const [mode, setMode] = useState<'data' | 'structure' | 'query'>('data');
    const [fullscreen, setFullscreen] = useState(localStorage.getItem("tw_fullscreen") ?? "");
    const [selectedItems, setSelectedItems] = useState<SelectedItems>(undefined);
    const [tableRightClicked, setTableRightClicked] = useState<RightClickTableEvent>(undefined);
    const [st, setSt] = useState<string | undefined>(undefined);
    const [orderBy, setOrderBy] = useState({});

    const [editingColumn, setEditingColumn] = useState<{ rowIndex: number, colIndex: number } | undefined>(undefined);
    const [editingColumnValue, setEditingColumnValue] = useState("");
    const [editingColumnCursorPosition, setEditingColumnCursorPosition] = useState(-1); // :InputCursorReset @Hack - We remember the cursor position within the input field as the cursor position is reset when we do a re-run of the table query.

    const [sqlEditorOpen, setSqlEditorOpen] = useState(false);
    const [sqlEditorResults, setSqlEditorResults] = useState<{ columns: any[], rows: any[] } | undefined>(undefined);

    const orderByString = (orderBy) => {
        if (orderBy[st] === undefined) return '';
        return `ORDER BY ${orderBy[st].columnName} ${orderBy[st].direction}`;
    }

    const { data: tables, isLoading: loadingTables } = useQuery<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, [], { once: false });
    const { data: rows } = useQuery<any[]>(`SELECT rowid, * FROM "${st}" ${orderByString(orderBy)} LIMIT 300`, [], { fireIf: st !== undefined, dependencies: [] });
    const { data: rowCount } = useQuery<{ count: number }>(`SELECT COUNT(*) AS count FROM "${st}"`, [], { fireIf: st !== undefined, first: true, dependencies: [] });
    const { data: columns } = useQuery<SqliteColumnInfo[]>(`PRAGMA table_info('${st}')`, [], { fireIf: st !== undefined });

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
            else if (show && focused) {
                if (e.key === 'Backspace' && selectedItems) {
                    if (selectedItems.type === 'row') {
                        if (resultTableFocused) {
                            handleDeleteItems();
                            handled = true;
                        }
                    } else {
                        handleDeleteItems();
                        handled = true;
                    }
                }
                if (e.key === 'Escape') {
                    deselectAll();
                    handled = true;
                }
                if (e.metaKey && e.key === 'a' && !editingColumn && resultTableFocused) {
                    selectAll();
                    handled = true;
                }
                if (e.ctrlKey && e.key === 's') {
                    setSqlEditorOpen(prev => !prev);
                    handled = true;
                }
                if (e.ctrlKey && e.key === 'f') {
                    toggleFullscreen();
                    handled = true;
                }
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
    }, [selectedItems, show, focused, resultTableFocused, fullscreen]);

    useEffect(() => {
        // :InputCursorReset
        const input = document.getElementById("tw_input_col_value");
        if (input) {
            input.selectionStart = editingColumnCursorPosition;
            input.selectionEnd = editingColumnCursorPosition;
        }
    }, [editingColumnCursorPosition])

    const toggleFullscreen = () => {
        if (!fullscreen) {
            setFullscreen("Y");
            localStorage.setItem("tw_fullscreen", "Y");
        } else {
            setFullscreen("");
            localStorage.setItem("tw_fullscreen", "");
        }
    }

    const onSqlEditorResults = (rows: any[]): void => {
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
        setEditingColumn(undefined);
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
        setEditingColumn(undefined);
    }

    const beginEditColumnValue = (rowIndex: number, colIndex: number) => {
        const row = rows[rowIndex];
        const colName = Object.keys(row)[colIndex + 1]; // +1 to skip rowid
        const existingValue = row[colName];

        setEditingColumn({ rowIndex, colIndex });
        setEditingColumnValue(existingValue);
        setSelectedItems(undefined);
    }

    const onColumnValueChange = async (e: ChangeEvent) => {
        const value = e.target.value;
        const cursorPos = e.target.selectionStart;

        if (editingColumn === undefined || st === undefined) return;
        const rowIndex = editingColumn.rowIndex;
        const colIndex = editingColumn.colIndex;

        const tblName = st;
        const row = rows[rowIndex];
        const rowId = row["rowid"];
        const colName = Object.keys(row)[colIndex + 1]; // +1 to skip rowid

        await db.exec(`UPDATE "${tblName}" SET ${colName} = ? WHERE rowid = ?`, [value, rowId]);

        setEditingColumnValue(value);
        setEditingColumnCursorPosition(cursorPos);
    }

    const rowValues = (row) => {
        const { rowid, ...rest } = row;
        return Object.values(rest);
    }

    const isEditingColumn = (rowIndex: number, colIndex: number) => {
        if (editingColumn === undefined) return false;
        if (editingColumn.rowIndex !== rowIndex) return false;
        if (editingColumn.colIndex !== colIndex) return false;
        return true;
    }

    const height = fullscreen ? "h-full" : "h-96"

    if (!show) return <></>
    return (
        <>
            <TableDropdown tables={tables} event={tableRightClicked} />
            <div
                className={`absolute bottom-0 w-full ${height} rounded-md bg-white cursor-default overflow-clip`}
                tabIndex={0}
                onClick={deselectAll}
                onFocus={() => setFocused(true)}
                onBlur={() => setFocused(false)}
            >
                <header className="flex p-1 justify-between items-center bg-gray-300 border-b border-gray-400">
                    <div>
                        <button onClick={() => setSqlEditorOpen(prev => !prev)} className="px-2 bg-gray-300 cursor-default hover:bg-gray-200" title="Open SQL Editor (ctrl+s)">
                            <p className="text-sm font-medium">SQL</p>
                        </button>
                    </div>
                    <div className="flex space-x-2 items-center">
                        {fullscreen && (
                            <button className="cursor-default" onClick={toggleFullscreen} title="Close fullscreen (ctrl+f)">
                                <CloseFullscreen className="w-6 h-6 fill-gray-500" />
                            </button>
                        )}
                        {!fullscreen && (
                            <button className="cursor-default" onClick={toggleFullscreen} title="Open fullscreen (ctrl+f)">
                                <OpenFullscreen className="w-6 h-6 fill-gray-500" />
                            </button>
                        )}
                        <button className="cursor-default" onClick={() => setShow(false)} title="Close (ctrl+d)"><XIcon className="w-8 h-8 fill-gray-500" /></button>
                    </div>
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
                                        <TableIcon className="w-6 h-6 fill-blue-400" />
                                        <p className="select-none">{table.name}</p>
                                    </div>
                                )
                            })}
                        </ul>
                    </section>
                    <div className="h-full w-full overflow-y-auto bg-white focus:outline-none" tabIndex={0} onFocus={() => setResultTableFocused(true)} onBlur={() => setResultTableFocused(false)}>
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
                            <tbody className="">
                                {mode === 'data' && rows && rows.map((row, rowIndex) => (
                                    <tr
                                        key={rowIndex}
                                        className={twMerge(`flex-1 h-8 truncate overflow-scroll ${rowIndex % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'} ${isSelected('row', rowIndex) && 'bg-blue-400 text-white'}`)}
                                        onClick={(e) => handleClickRow(e, rowIndex)}
                                    >
                                        {rowValues(row).map((v: any, colIndex) => (
                                            <td key={colIndex}>
                                                {isEditingColumn(rowIndex, colIndex) && (
                                                    <input
                                                        id="tw_input_col_value"
                                                        className="w-full border-r border-gray-50"
                                                        type="text"
                                                        name="col_value"
                                                        value={editingColumnValue}
                                                        onChange={onColumnValueChange}
                                                        onClick={(e) => e.stopPropagation()}
                                                        autoFocus
                                                    />
                                                )}
                                                {!isEditingColumn(rowIndex, colIndex) && (
                                                    <p className="border-r border-gray-50" onDoubleClick={() => beginEditColumnValue(rowIndex, colIndex)}>
                                                        {v}
                                                    </p>
                                                )}
                                            </td>
                                        ))}
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
                        <footer className="absolute bottom-8 pt-2 pb-4 mb-0 bg-gray-300 w-full">
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

                    <SqlEditor isOpen={sqlEditorOpen} fullscreen={fullscreen} onResults={onSqlEditorResults} />
                </div>
            </div>
        </>
    );
}

import CodeMirror from '@uiw/react-codemirror';
import { keymap } from "@codemirror/view";
import { sql as sqlLang, SQLite } from "@codemirror/lang-sql";
import { autocompletion } from "@codemirror/autocomplete";
import { githubLight } from "@uiw/codemirror-theme-github";

type SqlEditorProps = {
    isOpen: boolean
    fullscreen: boolean
    onResults: (rows: any[]) => void
}

const SqlEditor = ({ isOpen, fullscreen, onResults }: SqlEditorProps) => {
    const db = useDB();

    const [sql, setSql] = useState(localStorage.getItem("tw_sql_editor_query") ?? "");
    const [sqlError, setSqlError] = useState(undefined);

    const runSql = async (sql: string) => {
        const operation = sqlDetermineOperation(sql);
        if (operation === 'select' || operation === 'pragma' || operation === 'explain') {
            const { data, error } = await db.selectWithError(sql, []);
            if (error) {
                console.log(error);
                setSqlError(error.message);
            } else if (data.length === 0) {
                // @Improvement - Would be nice if we would still return an empty set of results with column headers. The reason
                // we can't is that we are basing the columns on the results. We could use the EXPLAIN keyword to get what columns are mapped.
                setSqlError("No results");
            }
            else {
                onResults(data as any[]);
                setSqlError(undefined);
            }
        } else {
            const err = await db.exec(sql, []);
            if (err) {
                setSqlError(err.message);
            } else {
                setSqlError(undefined);
            }
        }
    }

    const textChanged = (value: any) => {
        setSql(value);
        localStorage.setItem("tw_sql_editor_query", value);
    }

    const customKeymap = keymap.of([
        {
            key: "ctrl-Enter",
            run: () => {
                runSql(sql);
                return true;
            },
        },
    ]);

    const noCompletions = autocompletion({
        override: [
            () => null,
        ],
    });

    const editorHeight = () => { // @Hack - this is super hacky. Would wish that the height could just be 100%, but codeMirror says no. sigh...
        if (sqlError) {
            return fullscreen ? "88vh" : "270px"
        } else {
            return fullscreen ? "91vh" : "296px"
        }
    }

    if (!isOpen) return <></>
    return (
        <div className="w-full p-1 border-l-4 border-gray-300">
            {sqlError && (
                <div className="">
                    <p className="text-red-400">{sqlError}</p>
                </div>
            )}
            <CodeMirror
                value={sql}
                onChange={textChanged}
                extensions={[
                    sqlLang({ upperCaseKeywords: true, dialect: SQLite }),
                    noCompletions,
                    customKeymap,
                ]}
                height={editorHeight()}
                theme={githubLight}
                autoFocus={true}
            />
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