import { Outlet } from 'react-router-dom';
import { TableViewer } from "../components/TableViewer.tsx";

export const Layout = () => {
  return (
    <main>
        <Outlet />
        <TableViewer />
    </main>
  )
}
