// @deno-types="npm:@types/react@19"
import { useEffect, useState } from "react";
import { useDB, useQuery, useSyncer } from "@jakobsaadbye/teilen-sql/react";

type Props = {
    enabled: boolean
}

export const AutoSync = ({ enabled }: Props) => {
    const syncer = useSyncer();

    const changeCount = useQuery((db) => db.getChangeCount(), [], { tableDependencies: ["crr_changes", "crr_documents"] }).data ?? 0;

    // Push on any new changes
    useEffect(() => {
        if (changeCount > 0 && enabled) {
            setTimeout(() => {
                pushChanges();
            }, 10);
        }
    }, [changeCount, enabled]);

    // Handle incomming changes
    useEffect(() => {
        syncer.addEventListener("change", (event) => {
            // Here we could do some extra stuff with the changes we pull in, but
            // in this example we just let it be blank. The UI will automatically pick
            // up on the changes anyway
        });
        return () => {
            syncer.removeEventListener(() => {}); // @Cleanup - Fix
        }
    }, []);

    const pushChanges = () => {
        if (!enabled) return;
        syncer.pushChangesWs();
    }

    return;
}

