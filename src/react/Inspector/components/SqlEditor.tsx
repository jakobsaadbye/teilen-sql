import { useState } from "react";
import { useDB } from "../../hooks.ts";
import { sqlDetermineOperation } from "../../../utils.ts";
import CodeMirror from '@uiw/react-codemirror';
import { keymap } from "@codemirror/view";
import { sql as sqlLang, SQLite, SQLNamespace } from "@codemirror/lang-sql";
import { autocompletion } from "@codemirror/autocomplete";
import { githubDark, githubLight } from "@uiw/codemirror-theme-github";

type SqlEditorProps = {
    isOpen: boolean
    fullscreen: boolean
    onResults: (sql: string, rows: any[]) => void
}

export const SqlEditor = ({ isOpen, fullscreen, onResults }: SqlEditorProps) => {
    const db = useDB();

    const [sql, setSql] = useState(localStorage.getItem("tw_sql_editor_query") ?? "");
    const [useAutoComplete, setUseAutocomplete] = useState(localStorage.getItem("tw_sql_editor_autocomplete") ?? "Y");
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
                onResults(sql, data as any[]);
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

    const toggleAutoComplete = () => {
        const flipped = useAutoComplete == "Y" ? "" : "Y";
        setUseAutocomplete(flipped);
        localStorage.setItem("tw_sql_editor_autocomplete", flipped);
    }

    const customKeymap = keymap.of([
        {
            key: "ctrl-shift-Enter",
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

    const createCodemirrorSchema = (): SQLNamespace => {
        let result = {};

        for (const table of db.tables) {
            result[table.name] = {
                self: {
                    label: table.name,
                    type: "table"
                },
                children: table.columns.map(c => ({
                    label: c.name,
                    type: "column",
                    info: c.type.toLowerCase()
                }))
            }
        }

        return result;
    }

    const codeMirrorExtensions = [
        sqlLang({ upperCaseKeywords: true, dialect: SQLite, schema: createCodemirrorSchema() }),
        customKeymap,
    ];

    if (!useAutoComplete) {
        codeMirrorExtensions.push(noCompletions);
    }

    if (!isOpen) return <></>
    return (
        <div className="w-full p-1 border-l-4 border-gray-300">
            <header className="flex justify-between">
                <div className="">
                    {sqlError && <p className="text-red-400 text-sm">{sqlError}</p>}
                </div>
                <div className="flex gap-x-2 select-none" onClick={toggleAutoComplete}>
                    <label htmlFor="useAutoComplete" className="text-sm">Auto-complete</label>
                    <input name="useAutoComplete" type="checkbox" checked={useAutoComplete == "Y"} onChange={toggleAutoComplete} />
                </div>
            </header>
            <CodeMirror
                value={sql}
                onChange={textChanged}
                extensions={codeMirrorExtensions}
                height={fullscreen ? "91vh" : "276px"}
                theme={githubLight}
                autoFocus={true}
            />
        </div>
    )
}