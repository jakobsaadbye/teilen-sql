// @deno-types="@types/react"
import { useState } from 'react'
// @ts-expect-error Unable to infer type at the moment
import reactLogo from './assets/react.svg'
import trashcan from './assets/trashcan.svg'

import { useDB, useQuery, useSyncer } from "@jakobsaadbye/teilen-sql/react";
import { AutoSync } from "./AutoSync.tsx";

type Todo = {
  id: string
  title: string
  finished: boolean
  createdAt: string
}

function App() {

  const db = useDB();
  const syncer = useSyncer();

  const pushChanges = () => syncer.pushChangesHttp();
  const pullChanges = () => syncer.pullChangesHttp();

  const [autoPushEnabled, setAutoPushEnabled] = useState(false);

  const todos = useQuery<Todo[]>(`SELECT * FROM "todos" ORDER BY createdAt DESC`, []).data;


  const saveTodo = async (t: Todo) => {
    await db.execTrackChanges(`
      INSERT INTO "todos" (id, title, finished, createdAt)
        VALUES (?, ?, ?, ?)
      ON CONFLICT DO UPDATE SET
        title = EXCLUDED.title,
        finished = EXCLUDED.finished
    `, [t.id, t.title, t.finished ? 1 : 0, t.createdAt]);
  }

  const addTodo = async (e) => {
    e.preventDefault();

    const form = e.target;

    const todo: Todo = {
      id: crypto.randomUUID().split("-")[0],
      title: form.title.value,
      finished: false,
      createdAt: new Date().toString(),
    }

    await saveTodo(todo);

    // Reset form
    form.title.value = "";
  }

  const toggleFinished = async (t: Todo) => {
    t.finished = !t.finished;
    await saveTodo(t);
  }

  const renameTodo = async (t: Todo, newTitle: string) => {
    t.title = newTitle;
    await saveTodo(t);
  }

  const toggleAutoPush = () => {
    setAutoPushEnabled(!autoPushEnabled);
  }

  const clearAllTodos = async () => {
    await db.execTrackChanges(`DELETE FROM "todos" WHERE 1`, []);
  }

  return (
    <main className="min-h-screen items-center text-center text-white/90 bg-offblack">
      <div className="flex justify-center items-center gap-x-20 pt-20 mb-8">
        <a href="https://vite.dev" target="_blank">
          <img className="w-32" src="/vite.svg" alt="Vite logo" />
        </a>
        <a href="https://deno.com/" target="_blank">
          <img className="w-44" src="/vite-deno.svg" alt="Deno" />
        </a>
        <a href="https://reactjs.org" target="_blank">
          <img className="w-32 animate-spin transition ease-in-out duration-300 hover:scale-125 hover:drop-shadow-[0_0px_35px_rgba(30,70,255,0.30)]" src={reactLogo} alt="React logo" />
        </a>
        <a href="https://deno.com/" target="_blank">
          <img className="w-32 pt-8" src="/temporary_teilen_logo.png" alt="Teilen-sql" />
        </a>
      </div>
      <h1 className="text-6xl font-semibold">Vite + Deno + React + <span className="text-red-400">Teilen-sql</span></h1>
      <div>
        <p className="mt-8">
          Press <code className="bg-gray-950/40 p-1 rounded-lg">ctrl+i</code> to see the local database
        </p>
      </div>

      <div className="flex my-4 justify-center items-center gap-x-4">
        <form onSubmit={addTodo}>
          <input className="w-84 text-center p-2 border-b-1 border-white focus:outline-none" name="title" type="text" placeholder="What needs done?" />
          <input type="submit" hidden />
        </form>
      </div>

      <div className="flex justify-center">
        <div className="flex flex-col w-128 justify-center">

          <section className="my-4 flex justify-between">
            <div className="flex gap-x-2">
              <button type="button" className=" px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={pushChanges}>Push</button>
              <button type="button" className=" px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={pullChanges}>Pull</button>
              <div className="flex gap-x-4 px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={toggleAutoPush}>
                <label>Auto-push</label>
                <input type="checkbox" checked={autoPushEnabled} onChange={toggleAutoPush} />
              </div>
            </div>
            <button type="button" className="px-4 py-2 bg-red-600 rounded-lg" onClick={clearAllTodos}>
              <img src={trashcan} />
            </button>
          </section>

          <table className="border-1 border-white">
            <thead className="border-b-1 border-white">
              <tr className="">
                <th className="px-2 border-r-1 border-white">Id</th>
                <th className="px-2 border-r-1 border-white">Title</th>
                <th className="px-2 border-r-1 border-white">Finished</th>
              </tr>
            </thead>
            <tbody className="">
              {todos && todos.map((t, i) => (
                <tr key={i}>
                  <td className="px-2 border-r-1 border-white">{t.id}</td>
                  <td className="px-2 border-r-1 border-white">
                    <input type="text" value={t.title} onChange={e => renameTodo(t, e.target.value)} />
                  </td>
                  <td className="px-2 border-r-1 border-white">
                    <input className="scale-150" type="checkbox" checked={t.finished} onChange={e => toggleFinished(t)}></input>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <AutoSync enabled={autoPushEnabled} />
    </main>
  )
}

export default App
