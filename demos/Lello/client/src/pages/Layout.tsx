import { Outlet } from 'react-router-dom';
import { TableViewer } from '@teilen-sql/react/TableViewer/TableViewer.tsx'

export const Layout = () => {
  return (
    <main>
      <Outlet />
      <TableViewer />
    </main>
  )
}
