// @deno-types="@types/react"
import { useState } from 'react'
// @ts-expect-error Unable to infer type at the moment
import reactLogo from './assets/react.svg'
import trashcan from './assets/trashcan.svg'

import { useDB, useQuery, useSyncer } from "@jakobsaadbye/teilen-sql/react";
import { Commit, RowConflict } from "@jakobsaadbye/teilen-sql";

type Todo = {
  id: string
  title: string
  finished: boolean
  createdAt: string
}

function App() {

  const db = useDB();
  const syncer = useSyncer();

  const pushChanges = async () => {
    const r = await syncer.pushCommits(db);
    if (!r) return;

    if (r.status === "needs-pull") {
      // Apply small wiggle animation to pull button
      const pullBtn = document.getElementById("pull-btn");
      if (!pullBtn) return;

      pullBtn.classList.add("animate-wiggle");
      setTimeout(() => {
        pullBtn.classList.remove("animate-wiggle");
      }, 1500);
    }
  };
  const pullChanges = async () => {
    const r = await syncer.pullCommits(db);
    if (!r) {
      // Assume its because we have working changes
      const commitBtn = document.getElementById("commit-btn");
      if (!commitBtn) return;

      commitBtn.classList.add("animate-wiggle");
      setTimeout(() => {
        commitBtn.classList.remove("animate-wiggle");
      }, 1500);
    }
  };

  const todos = useQuery<Todo[]>(`SELECT * FROM "todos" ORDER BY createdAt DESC`, []).data;
  const commits = useQuery<Commit[]>(`SELECT * FROM "crr_commits" ORDER BY created_at DESC`, []).data;
  const conflicts = useQuery<RowConflict<Todo>[]>(db => db.getConflicts("todos"), [], { tableDependencies: ["crr_conflicts"] }).data;

  const changeCount = useQuery((db) => db.getUncommittedChangeCount("main"), [], { tableDependencies: ["crr_changes", "crr_documents"] }).data ?? 0;
  const pushCount = useQuery((db) => db.getPushCount("main"), [], { tableDependencies: ["crr_commits", "crr_documents"] }).data ?? 0;

  const commitChanges = async () => {
    await db.commit("");
  }

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

  const clearAllTodos = async () => {
    await db.execTrackChanges(`DELETE FROM "todos" WHERE 1`, []);
  }

  const hasConflicts = (t: Todo) : boolean => {
    if (!conflicts) return false;
    return conflicts.find(c => c.their.id === t.id) !== undefined;
  }

  const getConflictingTodo = (t: Todo): Todo | undefined => {
    if (!conflicts) return;
    const conflict = conflicts.find(c => c.their.id === t.id);
    if (!conflict) return;
    return conflict.their;
  }

  const acceptOurs = async (t: Todo) => {
    await db.resolveConflict("todos", t.id, "main", [["title", "our"]]);
  }

  const acceptTheir = async (t: Todo) => {
    await db.resolveConflict("todos", t.id, "main", [["title", "their"]]);
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
        <div className="flex flex-col w-160 justify-center">

          <section className="my-4 flex justify-between">
            <div className="flex gap-x-2">
              <button type="button" className="px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={pushChanges}>Push {pushCount ?? ""}</button>
              <button id="pull-btn" type="button" className="px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={pullChanges}>Pull</button>
              <button id="commit-btn" type="button" className="px-4 py-2 bg-gray-950/40 rounded-lg hover:opacity-80" onClick={commitChanges}>Commit {changeCount} changes</button>
            </div>
            <button type="button" className="px-4 py-2 bg-red-600 rounded-lg" onClick={clearAllTodos}>
              <img src={trashcan} />
            </button>
          </section>

          <div className="flex gap-x-8 items-start">

            <table className="border-1 border-white">
              <thead className="border-b-1 border-white">
                <tr className="">
                  <th className="px-2 border-r-1 border-white">Commit</th>
                </tr>
              </thead>
              <tbody>
                {commits && commits.map((c, i) => (
                  <tr key={i}>
                    <td className="px-2 border-r-1 border-white">{c.id}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <table className="border-1 border-white w-full">
              <thead className="border-b-1 border-white">
                <tr className="">
                  <th className="px-2 border-r-1 border-white">Id</th>
                  <th className="px-2 border-r-1 border-white">Title</th>
                  <th className="px-2 border-r-1 border-white">Finished</th>
                </tr>
              </thead>
              <tbody className="">
                {todos && todos.map((t, i) => (
                  <tr key={i} className="h-6">
                    <td className="px-2 border-r-1 border-white">{t.id}</td>
                    <td className="flex flex-col items-start border-r-1 border-white">
                      <div className={`flex px-2 justify-between items-center w-full ${hasConflicts(t) && "bg-git-green"}`}>
                        <input disabled={hasConflicts(t)} type="text" value={t.title} onChange={e => renameTodo(t, e.target.value)} />
                        {hasConflicts(t) && 
                        <p className="pr-2 text-sm text-baseline text-gray-400 select-none hover:text-gray-200" onClick={e => acceptOurs(t)}>
                          Accept
                        </p>}
                      </div>
                      {hasConflicts(t) && (
                        <div className="flex px-2 justify-between w-full bg-git-blue select-none">
                          <p className="text-start">{getConflictingTodo(t)?.title ?? ""}</p>
                          <p className="pr-2 text-sm text-baseline text-gray-400 hover:text-gray-200" onClick={e => acceptTheir(t)}>
                            Accept
                          </p>
                        </div>
                      )}
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
      </div>
    </main>
  )
}

export default App
