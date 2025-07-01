import { Context } from "@oak/oak";
import { applyChanges, Change, Document, SqliteDB } from "@jakobsaadbye/teilen-sql";

type MyWebSocket = WebSocket & {
    clientId: string
    db: SqliteDB
};

const connectedClients = new Map<string, MyWebSocket>();

export function handleStartWebSocketConnection(ctx: Context, db: SqliteDB) {
    const socket = ctx.upgrade() as MyWebSocket;
    const clientId = ctx.request.url.searchParams.get("clientId");

    if (!clientId) {
        socket.close(1008, "'clientId' was not provided as a search parameter");
        return;
    }

    socket.clientId = clientId;
    socket.db = db;

    socket.onopen = () => clientConnected(socket);
    socket.onclose = () => clientDisconnected.bind(clientId);
    socket.onmessage = (msg: MessageEvent) => handleMessage(socket, msg);

    connectedClients.set(clientId, socket);

    console.log(`New client connected ${clientId} ...`);
}

const clientConnected = (ws: MyWebSocket) => {
    console.log(`New connection opened ...`);

    const pullHintMsg = JSON.stringify({
        type: "pull-hint",
        data: "main" // docId
    });

    ws.send(pullHintMsg);
}

const clientDisconnected = (ws: MyWebSocket) => {
    console.log(`Client disconnected ${ws.clientId} ...`);
    connectedClients.delete(ws.clientId);
}

const handleMessage = (ws: MyWebSocket, m: MessageEvent) => {
    const msg = JSON.parse(m.data);
    
    switch (msg.type) {
        case "push-changes": handlePushChanges(ws, msg.data); break;
        case "pull-changes": handlePullChanges(ws, msg.data); break;
        default: {
            console.error(`Received unknown message '${msg.type}'`);
        }
    }
}

//
// @TODO: ***These two functions below should really be part of teilen as it should be standardized!***
//
const handlePullChanges = async (ws: MyWebSocket, document: Document) => {
    const clientId = ws.clientId;

    const pulledAt = new Date().getTime();
    const { data: changes, error } = await ws.db.selectWithError<Change[]>(`
        SELECT * FROM "crr_changes" WHERE document = ? AND site_id != ? AND applied_at > ?
    `, [document.id, clientId, document.last_pulled_at]);

    if (error) {
        const msg = JSON.stringify({
            type: "pull-changes-fail",
            data: error.message
        });

        ws.send(msg);
    } else {
        const pullOk = JSON.stringify({
            type: "pull-changes-ok",
            data: {
                document,
                changes,
                pulledAt
            }
        });

        ws.send(pullOk);
    }
}

const handlePushChanges = async (ws: MyWebSocket, data: { doc: Document, changes: Change[] }) => {
    try {
        await applyChanges(ws.db, data.changes);

        // Send back an OK message to the pushing client
        const pushOk = JSON.stringify({
            type: "push-changes-ok",
            data: data.doc
        });
        ws.send(pushOk);

        // Broadcast to everyone that a new change has occured on the document
        const pullHint = JSON.stringify({
            type: "pull-hint",
            data: data.doc.id
        });
        
        for (const client of connectedClients.values()) {
            if (client.clientId === ws.clientId) continue;

            client.send(pullHint);
        }

    } catch (error) {
        console.error(error);

        const err = JSON.stringify({
            type: "push-changes-fail",
            data: error.message
        });

        ws.send(err);
    }
}