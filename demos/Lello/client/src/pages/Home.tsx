import React from "react";
import { useNavigate } from "react-router";
import { useDB, useQuery } from "../../../teilen-sql/react.ts";
import { Board, Column } from "../db/types.ts";
import { BoardRepo } from "../db/repo/boardRepo.ts";
import { ControlBar } from "../components/ControlBar.tsx";
import { useState } from "react";
import { useIcon } from "../hooks/useIcon.ts";

export const Home = () => {
    const db = useDB();
    const navigate = useNavigate();

    const { data: boards, error } = useQuery<Board[]>(`SELECT * FROM "boards" ORDER BY "created_at" ASC`, []);

    if (error) return <p className='text-xl'>{error}</p>

    const createNewBoard = async () => {
        const boardId = crypto.randomUUID();
        navigate(`/boards/${boardId}`);

        const board: Board = {
            id: boardId,
            title: "Untitled",
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
        };
        await BoardRepo.saveBoard(db, board);

        const defaultColumns: Column[] = [
            { id: crypto.randomUUID(), board_id: boardId, title: "Backlog", position: "1" },
            { id: crypto.randomUUID(), board_id: boardId, title: "In-progress", position: "1" },
            { id: crypto.randomUUID(), board_id: boardId, title: "Finished", position: "1" },
        ];
        for (const col of defaultColumns) {
            await BoardRepo.saveColumn(db, col);
        };
    }

    return (
        <div className='relative h-dvh p-8 bg-sky-600 overflow-hidden'>
            <header>
                <h1 className="text-6xl text-white font-semibold">Lello</h1>
            </header>
            <ControlBar className="mt-8 mb-4" />
            <section className="flex flex-wrap gap-4 items-top cursor-default">
                {boards && boards.map((board, i) => (
                    <div onClick={() => navigate(`/boards/${board.id}`)} key={i} className={`flex justify-between w-80 h-48 p-2 bg-gray-200 border-2 border-gray-600 rounded-md hover:border-orange-400`}>
                        <h2 className="text-2xl text-gray-600">{board.title}</h2>
                        <BoardMenu boardId={board.id} />
                    </div>
                ))}
                <button onClick={createNewBoard} className="flex items-center justify-center w-80 h-16 p-2 border-2 border-gray-600 bg-gray-200 rounded-md hover:border-orange-400">
                    <p className="font-semibold text-gray-600">+ New board</p>
                </button>
            </section>
        </div>
    )
}

const BoardMenu = ({ boardId }: { boardId: string }) => {
    const db = useDB();
    const [menuIsOpen, setMenuIsOpen] = useState(false);
    const { MoreHorizontal, Trashcan } = useIcon();

    const remove = (e: PointerEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setMenuIsOpen(false);
        BoardRepo.deleteBoard(db, boardId);
    }

    const handleClick = (e: PointerEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setMenuIsOpen(!menuIsOpen);
    }

    return (
        <div className='relative' tabIndex={0} onBlur={() => setMenuIsOpen(false)}>
            <MoreHorizontal onClick={handleClick} className='fill-gray-500 w-8 h-8 hover:fill-gray-700 cursor-pointer' />
            {menuIsOpen && (
                <div className='absolute left-0'>
                    <div className='flex flex-col space-y-2 p-1 bg-gray-100 border-2 border-gray-400 rounded-lg'>
                        <div onClick={remove} tabIndex={1} className='flex py-2 px-4 space-x-1 bg-red-400 hover:bg-red-500 text-white rounded-md cursor-pointer' onMouseDown={e => e.preventDefault()}>
                            <Trashcan />
                            <p className='font-semibold'>Remove</p>
                        </div>
                    </div>
                </div>
            )}
        </div>
    )
}
