import { createContext, useContext, useEffect, useState } from 'react';
import { SqliteDB } from '../../src/sqlitedb.ts';
import { Syncer } from "../../src/syncer.ts";
import { sqlExplainQuery } from "../../src/utils.ts";

/**
 * Wrap the outermost component in the SqliteContext to allow child components to access the database which is required for all the hooks to work
 * 
 * Example:
 * ```ts 
    import { createDb } from "@jakobsaadbye/teilen-sql"
    import { SqliteContext } from "@jakobsaadbye/teilen-sql/react"

    const db = await createDb("example.db");

    createRoot(document.getElementById('root') as HTMLElement).render(
    <StrictMode>
        <SqliteContext.Provider value={db}>
            <App />
        </SqliteContext.Provider>
    </StrictMode>,
    )
 * ```
 */
export const SqliteContext = createContext<SqliteDB | null>(null);
export const SyncContext = createContext<Syncer | null>(null);

export const useDB = (): SqliteDB => {
    const db = useContext(SqliteContext);
    return db!;
}

export const useSyncer = (): Syncer => {
    return useContext(SyncContext)!;
}

// A QueryFunc is just any function that accepts the db as the first argument and returns data. 
// All the values in the 'params' list are passed to the function.
// Mostly used if you want to delegate out complex queries into a function that lives elsewhere. 
// NOTE(*important*): You need to specify the list of table dependencies for the function to re-run in the query options
type QueryFunc<T> = (db: SqliteDB, ...params: any) => Promise<T>;

type UseQueryOptions = {
    fireIf?: boolean        // A condition to be true before executing
    once?: boolean          // If the query only should run once when the component mounts
    first?: boolean         // Get the first matching result, undefined if no result
    dependencies?: string[] // List of table names that if updated re-runs the query. Only needed to be specified if passed a function that can run arbitrary sql stmts. Otherwise the affected table is infered from the sql query
}

export const useQuery = <T>(sql: string | QueryFunc<T>, params: any[], options?: UseQueryOptions) : {data: T, error: any, isLoading: boolean} => {
    const db = useContext(SqliteContext);
    if (db === null) {
        throw new Error(`Failed to retreive db from context. Make sure the components useQuery is used in, is inside of a SqliteContext.Provider`)
    }

    const [data, setData] = useState<T>(undefined);
    const [error, setError] = useState(undefined);
    const [isLoading, setIsLoading] = useState(true);

    const [counter, setCounter] = useState(0); // Used to re-run the effect on a table change
    const rerender = () => setCounter(counter + 1);

    useEffect(() => {
        let dependencies: string[] | undefined = undefined;
        if (typeof(sql) === 'function') dependencies = options?.dependencies ?? [];
        else {
            if (options && options.dependencies) dependencies = options.dependencies;
            else {
                sqlExplainQuery(db, sql)
                    .then(deps => dependencies = deps)
                    .catch(() => dependencies = []);
            }
        } 

        if (options?.fireIf === false) return;

        let isMounted = true;

        const fire = () => {
            let run;
            if (typeof (sql) === 'string') {
                run = db.select<T>(sql, params);
            } else if (typeof (sql) === 'function') {
                let fn = sql as (db: SqliteDB, ...params: any) => T;
                run = fn(db, ...params);
            }

            run
                .then(data => {
                    if (!isMounted) return;
                    if (options?.first) {
                        if (data.length > 0) {
                            setData(data[0]);
                        } else {
                            setData(undefined);
                        }
                    } else {
                        setData(data);
                    }
                    setError(undefined);
                    setIsLoading(false);
                    rerender();
                })
                .catch(err => {
                    if (!isMounted) return;
                    setData(undefined);
                    setError(err.message);
                    setIsLoading(false);
                    rerender();
                });
        };
        fire();

        // Re-run query if dependent tables changes
        const bc = new BroadcastChannel("table_change");
        if (options?.once) {
            // Skip
        } else {
            bc.addEventListener('message', (e) => {
                const changedTable: string = e.data;
                if (changedTable === "") return fire();
                if (dependencies.length > 0) {
                    if (dependencies.includes(changedTable)) {
                        fire();
                    }
                } else {
                    fire();
                }
            });
        }

        return () => {
            isMounted = false;
            bc.close();
        }
    }, [sql, options?.fireIf]);

    return { data, error, isLoading };
}