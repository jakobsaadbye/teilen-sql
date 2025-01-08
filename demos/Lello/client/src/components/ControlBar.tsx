import React from "react";
import { Change, getChangeSets } from "@teilen-sql/change.ts";
import { useQuery, useSyncer } from "@teilen-sql/react/hooks.ts";
import { SqliteDB } from "@teilen-sql/sqlitedb.ts";
import { useGoBananas } from "../hooks/monkey.ts";
import { useIcon } from "../hooks/useIcon.ts"
import { twMerge } from 'tailwind-merge'

const changesCount = async (db: SqliteDB) => {
    const client = await db.first(`SELECT * FROM "crr_clients" WHERE site_id = $1`, [db.siteId]);
    if (!client) return -1;
    const lastPushedAt = client.last_pushed_at;

    const rows = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > $1 AND site_id = $2`, [lastPushedAt, db.siteId]);
    const changeSets = getChangeSets(rows);
    return changeSets.length;
}

type Props = {
    boardId?: string
    className?: string
}

export const ControlBar = ({ boardId, className }: Props) => {
    const [goBananas, cancel, running] = useGoBananas(boardId);

    const changeCount = useQuery<number | undefined>(changesCount, []).data;

    const syncer = useSyncer("http://127.0.0.1:3000/changes");
    const pullChanges = () => syncer.pullChanges();
    const pushChanges = () => syncer.pushChanges();

    const { ArrowUp, ArrowDown, Rabbit, Block } = useIcon();

    return (
        <div className={twMerge("flex space-x-2 items-center text-center", className)}>
            <button title="Pull changes" onClick={pullChanges} className="flex space-x-2 py-2 px-4 bg-gray-200 border border-gray-400 cursor-default rounded-md hover:bg-gray-100">
                <p className="text-gray-600">Pull</p>
                <ArrowDown className="w-6 h-6 fill-gray-600" />
            </button>
            <button title="Push changes" onClick={pushChanges} className="flex space-x-2 py-2 px-4 bg-gray-200 border border-gray-400 cursor-default rounded-md hover:bg-gray-100">
                <p className="text-gray-600">Push</p>
                <ArrowUp className="w-6 h-6 fill-gray-600" />
            </button>
            <h2 className="text-2xl text-white font-semibold">{changeCount} changes</h2>
            {boardId && !running && <button title="Go banannas" className="cursor-default" onClick={() => goBananas(30000)}><Rabbit className="w-12 h-12 fill-gray-200"/></button>}
            {boardId && running && <Block className="w-12 h-12 fill-gray-200" onClick={cancel} />}
        </div>
    )
}
