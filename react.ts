import { createContext, useContext, useEffect, useState } from 'react';
import { SqliteDB } from './sqlitedb.ts';
import { Syncer } from "./syncer.ts";
import { sqlExplainQuery } from "./change.ts";

export const SqliteContext = createContext<SqliteDB | null>(null);

export const useDB = (): SqliteDB => {
    const db = useContext(SqliteContext);
    return db!;
}

export const useSyncer = (endpoint: string): Syncer => {
    const db = useDB();
    return new Syncer(db, endpoint);
}

type UseQueryOptions = {
    fireIf?: boolean        // A condition to be true before executing
    once?: boolean          // If the query only should run once when the component mounts
    first?: boolean         // Get the first matching result, undefined if no result
    dependencies?: string[] // List of table names that if updated re-runs the query. Only needed to be specified if passed a function that can run arbitrary sql stmts. Otherwise the affected table is infered from the sql query
}

export const useQuery = <T>(sql: string | ((db: SqliteDB, ...params: any) => Promise<T>), params: any[], options?: UseQueryOptions) : {data: T, error: any, isLoading: boolean} => {
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
        else sqlExplainQuery(db, sql).then(deps => dependencies = deps).catch(() => dependencies = []);

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
                const changedTable = e.data;
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