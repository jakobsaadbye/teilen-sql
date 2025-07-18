// @TODO:
//    - Fix z-index overlapping with underlying application. Maybe pick some high z-indicies that users unlikely will use

import type { ChangeEvent, KeyboardEvent, PointerEvent } from "react";
import { useEffect, useState } from "react";
import { twMerge } from "tailwind-merge";
import { useDB, useQuery } from "../hooks.ts"

import type { SelectedItems, RightClickTableEvent, RightClickDataEvent } from "./types.ts";
import type { SqliteColumnInfo } from "@/src/sqlitedb.ts";

import { TableDropdown } from "./components/TableDropdown.tsx";
import { DataDropdown } from "./components/DataDropdown.tsx";
import { SqlEditor } from "./components/SqlEditor.tsx";

import XIcon from "./icons/X.tsx";
import TableIcon from "./icons/Table.tsx";
import TableGroupIcon from "./icons/TableGroup.tsx";
import ChevronUp from "./icons/ChevronUp.tsx";
import ChevronDown from "./icons/ChevronDown.tsx";
import ChevronRight from "./icons/ChevronRight.tsx";
import OpenFullscreen from "./icons/OpenFullscreen.tsx";
import CloseFullscreen from "./icons/CloseFullscreen.tsx";
import SettingsIcon from "./icons/Settings.tsx";
import { SqliteDB } from "../../sqlitedb.ts";

type TableGroup = {
    name: string
    tables: string[]
}

const getAllTables = async (db: SqliteDB) => {
    const tables = await db.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
    return tables.map(r => r.name);
}

export const Inspector = ({ children }) => {
    const db = useDB();

    const [show, setShow] = useState(false);
    const [focused, setFocused] = useState(true);
    const [resultTableFocused, setResultTableFocused] = useState(true);
    const [mode, setMode] = useState<'data' | 'structure' | 'query'>('data');
    const [fullscreen, setFullscreen] = useState(localStorage.getItem("tw_fullscreen") ?? "");
    const [selectedItems, setSelectedItems] = useState<SelectedItems>(undefined);
    const [tableRightClicked, setTableRightClicked] = useState<RightClickTableEvent>(undefined);
    const [dataRightClicked, setDataRightClicked] = useState<RightClickDataEvent>(undefined);
    const [st, setSt] = useState<string | undefined>(undefined); // selected table
    const [orderBy, setOrderBy] = useState({});

    const [editingColumn, setEditingColumn] = useState<{ rowIndex: number, colIndex: number } | undefined>(undefined);
    const [editingColumnValue, setEditingColumnValue] = useState("");
    const [editingColumnCursorPosition, setEditingColumnCursorPosition] = useState(-1); // :InputCursorReset @Hack - We remember the cursor position within the input field as the cursor position is reset when we do a re-run of the table query.

    const [sqlEditorOpen, setSqlEditorOpen] = useState(false);
    const [sqlEditorResults, setSqlEditorResults] = useState<{ sql: string, columns: any[], rows: any[] } | undefined>(undefined);

    const orderByString = (orderBy) => {
        if (orderBy[st] === undefined) return '';
        return `ORDER BY ${orderBy[st].columnName} ${orderBy[st].direction}`;
    }

    const currentQuery = () => {
        return `SELECT rowid, * FROM "${st}" ${orderByString(orderBy)} LIMIT 300`;
    }

    const currentQueryNoRowid = () => {
        return `SELECT * FROM "${st}" ${orderByString(orderBy)} LIMIT 300`;
    }

    const allTables = useQuery(getAllTables, [], { once: false, tableDependencies: ["sqlite_master"] }).data ?? [];

    const { data: rows } = useQuery<any[]>(currentQuery(), [], { fireIf: st !== undefined, tableDependencies: [] });
    const { data: rowCount } = useQuery<{ count: number }>(`SELECT COUNT(*) AS count FROM "${st}"`, [], { fireIf: st !== undefined, first: true, tableDependencies: [] });
    const { data: columns } = useQuery<SqliteColumnInfo[]>(`PRAGMA table_info('${st}')`, [], { fireIf: st !== undefined });

    const userTables = allTables.filter(t => !db.frameworkMadeTables.includes(t));
    const systemTables = allTables.filter(t => db.frameworkMadeTables.includes(t));

    const teilenGroup: TableGroup = {
        name: "teilen",
        tables: systemTables
    };

    const [showTeilenGroup, setShowTeilenGroup] = useState(false);

    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            let handled = false;
            if (e.ctrlKey && e.key === 'i') {
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

                // if (e.ctrlKey && e.key === 's') {
                //     setSqlEditorOpen(prev => !prev);
                //     handled = true;
                // }

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
    }, [editingColumnCursorPosition]);

    const toggleFullscreen = () => {
        if (!fullscreen) {
            setFullscreen("Y");
            localStorage.setItem("tw_fullscreen", "Y");
        } else {
            setFullscreen("");
            localStorage.setItem("tw_fullscreen", "");
        }
    }

    const onSqlEditorResults = (sql: string, rows: any[]): void => {
        if (rows.length === 0) return;
        const columns = Object.keys(rows[0]);
        setSqlEditorResults({ sql, columns, rows });
        setMode('query');
    }

    const handleDeleteItems = async () => {
        if (selectedItems === undefined) return;
        if (selectedItems.type === 'table') {
            for (const tableName of selectedItems.items) {
                const err = await db.exec(`DROP TABLE IF EXISTS ${tableName}`, []);
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
        setDataRightClicked(undefined);
        setEditingColumn(undefined);
    }

    const isSelected = (type: 'table' | 'row', index: string) => {
        if (selectedItems === undefined) return false;
        if (selectedItems.type !== type) return false;

        return selectedItems.items.findIndex(idx => idx === index) !== -1;
    }

    const handleClickTable = (e: PointerEvent, tableName: string) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === "click") {
            if (e.shiftKey) {
                if (selectedItems === undefined || selectedItems.type !== 'table') {
                    setSelectedItems({ type: 'table', items: [tableName] });
                } else {
                    const selectedTable = selectedItems.items[0] as string;

                    const anchorA = allTables.indexOf(selectedTable);
                    const anchorB = allTables.indexOf(tableName);

                    const between: string[] = [];
                    if (anchorA < anchorB) {
                        for (let i = anchorA; i < anchorB; i++) between.push(allTables[i]);
                    } else {
                        for (let i = anchorA; i > anchorB; i--) between.push(allTables[i]);
                    }

                    setSelectedItems({ type: 'table', items: [selectedTable, ...between, tableName] });
                }
            } else {
                deselectAll();
                setSt(tableName);
                setSelectedItems({ type: 'table', items: [tableName] });
            }
            if (mode === 'query') {
                setMode('data');
            }
        }
        if (e.type === "contextmenu") {
            setTableRightClicked({ tableName: tableName, mouseX: Math.floor(e.clientX), mouseY: Math.floor(e.clientY) });
        }
    }

    const handleRightClickedDataTable = (e: PointerEvent) => {
        e.preventDefault();
        e.stopPropagation();

        let sql = currentQueryNoRowid();
        if (sqlEditorResults && mode === "query") {
            sql = sqlEditorResults.sql;
        }

        setDataRightClicked({
            sql: sql,
            mouseX: Math.floor(e.clientX),
            mouseY: Math.floor(e.clientY)
        });
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
                deselectAll();
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

    const deleteEntireDatabase = async () => {
        const opfsRoot = await navigator.storage.getDirectory();
        await opfsRoot.removeEntry(db.name);
    }

    const height = fullscreen ? "h-full" : "h-96"

    if (!show) return <>{children}</>
    return (
        <>
            {children}

            <TableDropdown event={tableRightClicked} />
            <DataDropdown event={dataRightClicked} />

            <div
                className={`absolute bottom-0 border-t-1 border-t-gray-400 w-full ${height} rounded-md bg-white cursor-default overflow-clip`}
                tabIndex={0}
                onClick={deselectAll}
                onFocus={() => setFocused(true)}
                onBlur={() => setFocused(false)}
            >
                <header className="flex p-1 justify-between items-center bg-gray-300 border-b border-gray-400">
                    <div className="flex gap-x-2">
                        <div>
                            <button onClick={() => setSqlEditorOpen(prev => !prev)} className="px-2 bg-gray-300 cursor-default hover:bg-gray-200 rounded-sm" title="Open SQL Editor (ctrl+s)">
                                <p className="text-sm font-medium">SQL</p>
                            </button>
                        </div>
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
                        <button className="cursor-default" onClick={() => setShow(false)} title="Close (ctrl+i)"><XIcon className="w-8 h-8 fill-gray-500" /></button>
                    </div>
                </header>
                <div className="relative flex h-full w-full">
                    <section className="min-w-48 px-2 pb-12 bg-gray-300 overflow-y-auto">
                        <h2 className="text-lg">Tables</h2>
                        <ul className="ml-1 mr-2 mt-1">
                            {userTables.map((table, i) => {
                                return (
                                    <TableItem
                                        table={table}
                                        index={i}
                                        onLeftClick={e => handleClickTable(e, table)}
                                        onRightClick={e => handleClickTable(e, table)}
                                        selected={isSelected('table', table)}
                                    />
                                )
                            })}

                            {/* Teilen-SQL tables */}
                            <div className="">
                                <div className="flex items-center gap-x-1 pr-2 hover:bg-gray-200 rounded-sm" onClick={() => setShowTeilenGroup(!showTeilenGroup)}>
                                    <TableGroupIcon className="min-w-6 min-h-6 fill-blue-400" />
                                    <p className="select-none truncate">{teilenGroup.name}</p>
                                    {showTeilenGroup && <ChevronDown className="w-5 h-5 fill-gray-400" />}
                                    {!showTeilenGroup && <ChevronRight className="w-5 h-5 fill-gray-400" />}
                                </div>
                                {showTeilenGroup && (
                                    <ul className="pl-4">
                                        {teilenGroup.tables.map((table, i) => {
                                            return (
                                                <TableItem
                                                    table={table}
                                                    index={i}
                                                    onLeftClick={e => handleClickTable(e, table)}
                                                    onRightClick={e => handleClickTable(e, table)}
                                                    selected={isSelected('table', table)}
                                                />
                                            )
                                        })}
                                    </ul>
                                )}
                            </div>

                        </ul>
                    </section>
                    <div className="h-full w-full overflow-y-auto bg-white focus:outline-none" tabIndex={0} onContextMenu={handleRightClickedDataTable} onFocus={() => setResultTableFocused(true)} onBlur={() => setResultTableFocused(false)}>
                        <table className="flex-1 w-full bg-white mb-32">
                            <thead className="flex-1 bg-gray-200 sticky top-0 w-full">
                                <tr className="w-full">
                                    {mode === 'data' && columns && (columns.map((c, i) => (
                                        <th onClick={() => orderByColumn(c.name)} key={i} className="px-1 border-r border-gray-50">
                                            <div className="flex justify-center items-center">
                                                <p className="text-center font-semibold">{c.name}</p>
                                                {orderBy[st]?.columnName === c.name && orderBy[st]?.direction === 'ASC' && <ChevronUp className="w-4 h-4 fill-gray-600" />}
                                                {orderBy[st]?.columnName === c.name && orderBy[st]?.direction === 'DESC' && <ChevronDown className="w-4 h-4 fill-gray-600" />}
                                            </div>
                                        </th>
                                    )))}
                                    {mode === 'structure' && columns && Object.keys(columns[0]).map((name, i) => (
                                        <th key={i} className="px-1 border-r border-gray-50">
                                            {name}
                                        </th>
                                    ))}
                                    {mode === 'query' && sqlEditorResults && sqlEditorResults.columns.map((name, i) => (
                                        <th key={i} className="px-1 border-r border-gray-50">
                                            {name}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="select-none">
                                {mode === 'data' && rows && rows.map((row, rowIndex) => (
                                    <tr
                                        key={rowIndex}
                                        className={twMerge(`flex-1 h-8 truncate overflow-scroll ${rowIndex % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'} ${isSelected('row', rowIndex) && 'bg-blue-500 text-white'}`)}
                                        onClick={(e) => handleClickRow(e, rowIndex)}
                                    >
                                        {rowValues(row).map((v: any, colIndex) => (
                                            <td className="px-1 border-r border-gray-50" key={colIndex} onDoubleClick={() => beginEditColumnValue(rowIndex, colIndex)}>
                                                {isEditingColumn(rowIndex, colIndex) && (
                                                    <input
                                                        id="tw_input_col_value"
                                                        className={twMerge("w-full", v === null && "text-gray-400")}
                                                        type="text"
                                                        name="col_value"
                                                        value={editingColumnValue}
                                                        onChange={onColumnValueChange}
                                                        onClick={(e) => e.stopPropagation()}
                                                        autoFocus
                                                    />
                                                )}
                                                {!isEditingColumn(rowIndex, colIndex) && (
                                                    <>
                                                        {v === null && (<p className="text-gray-400">NULL</p>)}
                                                        {v !== null && (<p>{v}</p>)}
                                                    </>
                                                )}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                                {mode === 'structure' && columns && columns.map((col, i) => (
                                    <tr key={i} className={`flex-1 h-8 truncate overflow-scroll ${i % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'}`}>
                                        {Object.values(col).map((v: any, i) => <td key={i} className="px-1 border-r border-gray-50">{v}</td>)}
                                    </tr>
                                ))}
                                {mode === 'query' && sqlEditorResults && sqlEditorResults.rows.map((row, i) => (
                                    <tr key={i} className={`flex-1 h-8 truncate overflow-scroll ${i % 2 === 0 ? 'bg-gray-100' : 'bg-gray-200'}`}>
                                        {Object.values(row).map((v: any, i) => <td key={i} className="px-1 border-r border-gray-50">{v}</td>)}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        <footer className="absolute bottom-8 pt-2 pb-4 mb-0 bg-gray-300 w-full">
                            <div className="flex justify-between select-none">
                                <div className="flex space-x-2">
                                    <p onClick={() => setMode('data')} className={`px-4 rounded-sm ${mode === 'data' && 'bg-gray-100'}`}>Data</p>
                                    <p onClick={() => setMode('structure')} className={`px-4 rounded-sm ${mode === 'structure' && 'bg-gray-100'}`}>Structure</p>
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

type Props = {
    table: string
    index: number
    onLeftClick: () => void
    onRightClick: () => void
    selected: boolean
}

const TableItem = ({ table, index, onLeftClick, onRightClick, selected }: Props) => {
    return (
        <div
            onClick={onLeftClick}
            onContextMenu={onRightClick}
            className={twMerge(`flex gap-x-1 pr-2 hover:bg-gray-200 rounded-sm`, selected && 'bg-gray-100')}
        >
            <TableIcon className="min-w-6 min-h-6 fill-blue-400" />
            <p className="select-none truncate">{table}</p>
        </div>
    )
}

