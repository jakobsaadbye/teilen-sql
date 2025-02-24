import { Outlet } from 'react-router-dom';
import { Inspector } from "@teilen-sql-react"

export const Layout = () => {
  return (
    <Inspector >
      <main>
        <Outlet />
      </main>
    </Inspector>
  )
}
