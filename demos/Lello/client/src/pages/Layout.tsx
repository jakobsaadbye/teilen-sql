import { Outlet } from 'react-router-dom';
import { TableViewer } from "@teilen-sql-react"

export const Layout = () => {
  return (
    <main>
      <Outlet />
      <TableViewer />
    </main>
  )
}
