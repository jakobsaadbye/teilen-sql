import { createContext, useContext, useEffect, useState } from 'react';
import { SqliteDB } from './sqlitedb.ts';
import { Syncer } from "./syncer.ts";

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
    dependency?: boolean // A condition to be true before executing
    once?: boolean // If the query only should run once when the component mounts
    first?: boolean // Get the first matching result or undefined
}

export const useQuery = <T>(sql: string | ((db: SqliteDB, ...params: any) => Promise<T>), params: any[], options?: UseQueryOptions) => {
    const db = useContext(SqliteContext);
    if (db === null) {
        throw new Error(`Failed to retreive db from context. Make sure the components useSelect is used in, is inside of a SqliteContext.Provider`)
    }

    const [data, setData] = useState<T>(undefined);
    const [error, setError] = useState(undefined);
    const [isLoading, setIsLoading] = useState(true);

    const [counter, setCounter] = useState(0); // Used to re-run the effect on a table change
    const rerender = () => setCounter(counter + 1);


    useEffect(() => {
        if (options?.dependency === false) return;

        let isMounted = true;

        const goQuery = () => {
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
        goQuery();

        // Re-run query if dependent tables changes
        const bc = new BroadcastChannel("table_change");
        if (options?.once) {
            // Skip
        } else {
            bc.addEventListener('message', () => {
                goQuery();
            });
        }

        return () => {
            isMounted = false;
            bc.close();
        }
    }, [sql, options?.dependency]);

    return { data, error, isLoading };
}