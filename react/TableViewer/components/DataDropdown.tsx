import { useEffect, useRef, useState } from "react";
import { useDB } from "../../hooks.ts";
import type { RightClickDataEvent } from "../types.ts";

type Props = {
    event?: RightClickDataEvent
}

export const DataDropdown = ({ event }: Props) => {
    const db = useDB();

    const menuRef = useRef<HTMLElement | null>(null);

    const exportCurrentPage = async () => {
        if (!event) return

        const rows = await db.select<any[]>(event.sql, []);

        const jsonData = JSON.stringify(rows);

        const link = document.createElement("a");
        const file = new Blob([jsonData], { type: 'text/plain' });
        link.href = URL.createObjectURL(file);
        link.download = `page.json`;
        link.click();
        URL.revokeObjectURL(link.href);
    }

    const items = [
        { name: "Export current page...", onClick: exportCurrentPage },
    ];

    useEffect(() => {
        if (menuRef.current === null) return
        if (event === undefined) return

        menuRef.current.style.top = `${event.mouseY}px`
        menuRef.current.style.left = `${event.mouseX}px`
    }, [event])

    if (!event) return <></>
    return (
        <>
            <div ref={menuRef} className={`absolute z-50 p-1 bg-gray-200 border border-gray-400 w-64`}>
                <div className="flex flex-col space-y-2">
                    {items.map((item, i) => (
                        <button key={i} onClick={item.onClick} className="pl-1 text-start w-full border-b-gray-400 cursor-default hover:bg-gray-100">{item.name}</button>
                    ))}
                </div>
            </div>
        </>
    );
}

// type ExportState = {
//     format: "csv" | "json" | "sql"
// }

// type ExportCurrentPageProps = {
//     show: boolean
// }

// const ExportCurrentPage = ({ show }: ExportCurrentPageProps) => {

//     const [state, setState] = useState<ExportState>({
//         format: "sql"
//     });

//     const exportCurrentPage = () => {

//     }

//     if (!show) return <></>

//     console.log("Show export current page!");

//     return (
//         <div className="absolute inset-0 flex items-center justify-center z-50">
//             <div className="w-[400px] h-[600px] bg-pink-800">
//                 <p>Hellope!</p>
//             </div>
//         </div>
//     )
// }
