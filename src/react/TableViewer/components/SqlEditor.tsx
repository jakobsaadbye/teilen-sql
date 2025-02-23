import { useState } from "react";
import { useDB } from "../../hooks.ts";
import { sqlDetermineOperation } from "../../../../src/utils.ts";
import CodeMirror from '@uiw/react-codemirror';
import { keymap } from "@codemirror/view";
import { sql as sqlLang, SQLite } from "@codemirror/lang-sql";
import { autocompletion } from "@codemirror/autocomplete";
import { githubLight } from "@uiw/codemirror-theme-github";

type SqlEditorProps = {
    isOpen: boolean
    fullscreen: boolean
    onResults: (sql: string, rows: any[]) => void
}

export const SqlEditor = ({ isOpen, fullscreen, onResults }: SqlEditorProps) => {
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