import './App.css'
// @deno-types="@types/react"
import { useState } from 'react'
// @ts-expect-error Unable to infer type at the moment
import reactLogo from './assets/react.svg'

function App() {
  const [count, setCount] = useState(0)

  return (
    <main className="w-[100vw] h-[100vh] items-center text-center text-white/90 bg-offblack">
      <img className="justify-self-center" src="/vite-deno.svg" alt="Vite with Deno" />
      <div className="flex justify-center gap-x-20 mb-8">
        <a href="https://vite.dev" target="_blank">
          <img className="w-32" src="/vite.svg" alt="Vite logo" />
        </a>
        <a href="https://reactjs.org" target="_blank">
          <img className="w-32 animate-spin transition ease-in-out duration-300 hover:scale-150 hover:drop-shadow-[0_0px_35px_rgba(30,70,255,0.30)]" src={reactLogo} alt="React logo" />
        </a>
      </div>
      <h1 className="text-6xl font-semibold">Vite + React + Teilen-sql</h1>
      <div>
        <button className="my-4 p-2 bg-gray-950/40 rounded-lg" onClick={() => setCount((count) => count + 1)}>
          count is {count}
        </button>
        <p>
          Edit <code className="bg-gray-950/40 p-1 rounded-lg">src/App.tsx</code> and save to test HMR
        </p>
      </div>
      <p className="read-the-docs">
        Click on the Vite and React logos to learn more
      </p>
    </main>
  )
}

export default App
