import React from "react";
import { useState } from 'react';
import { useParams } from "react-router";
import { useIcon } from '../hooks/useIcon.ts';
import { useDB, useQuery } from '../../../teilen-sql/react.ts';
import { BoardRepo } from '../db/repo/boardRepo.ts';
import { Todo, Column, BoardDetail, ColumnDetail, Board } from '../db/types.ts';
import { ControlBar } from '../components/ControlBar.tsx';

type DragType = 'none' | 'column' | 'todo'

type Context = {
    boardId: string,
    board: BoardDetail,
    onTodoDrag: (columnId: string, todoId: string) => void
    onTodoDrop: (columnId: string, todoId: string) => void
    onTodoDropBottomSpace: (columnId: string) => void
    onColumnDrag: (columnId: string) => void
    onColumnDrop: (columnId: string) => void
    currentDragType: DragType
    setCurrentDragType: (dt: DragType) => void
    autoFocus: {
        currentId: string,
        setId: (id: string) => void
    }
};

export const BoardPage = () => {
    const { id } = useParams();

    const db = useDB();

    const { data: board, error } = useQuery<BoardDetail>(BoardRepo.getBoard, [id], { dependencies: ["boards", "columns", "todos"] });

    const columns = board?.columns ?? [];
    const [currentDragType, setCurrentDragType] = useState<DragType>('none')
    const [draggedTodo, setDraggedTodo] = useState<DragTodo | null>(null);
    const [draggedColumnId, setDraggedColumnId] = useState<string | undefined>(undefined);
    const [currentFocusedId, setCurrentFocusedId] = useState("");

    const onColumnDrag = (columnId: string) => {
        setDraggedColumnId(columnId);
    }

    const onTodoDrag = (columnId: string, todoId: string) => {
        setDraggedTodo({
            columnId: columnId,
            todoId: todoId
        });
    }

    const onColumnDrop = (afterId: string) => {
        const srcColumn = board.columns.find(c => c.id === draggedColumnId);
        if (srcColumn === undefined) return assertFailed();
        BoardRepo.saveColumn(db, { ...srcColumn, position: afterId });
        return;
    }

    const onTodoDrop = (columnId: string, afterTodoId: string) => {
        const srcColumn = board.columns.find(c => c.id === draggedTodo!.columnId);
        const srcTodo = srcColumn!.todos.find(t => t.id === draggedTodo!.todoId);
        if (srcColumn === undefined || srcTodo === undefined) return assertFailed();
        BoardRepo.saveTodo(db, { ...srcTodo, column_id: columnId, position: afterTodoId });
    }

    const onTodoDropBottomSpace = (columnId: string) => {
        const columnDst = columns.find((c, _) => c.id === columnId);
        if (columnDst === undefined) { assertFailed(); return; }
        if (columnDst.todos.length === 0) {
            onTodoDrop(columnId, "-1");
        } else {
            onTodoDrop(columnId, "1");
        }
    }

    const ctx: Context = {
        boardId: id,
        onTodoDrag,
        onTodoDrop,
        onTodoDropBottomSpace,
        onColumnDrag,
        onColumnDrop,
        currentDragType,
        setCurrentDragType,
        autoFocus: {
            currentId: currentFocusedId,
            setId: setCurrentFocusedId
        }
    } as Context

    const addColumn = () => {
        const column = {
            id: crypto.randomUUID(),
            board_id: board.id,
            position: "1",
            title: "",
        } as Column

        BoardRepo.saveColumn(db, column);
        ctx.autoFocus.setId(column.id);
    }

    const updateTitle = (title: string) => {
        BoardRepo.saveBoard(db, { ...board, title });
    }

    if (error) return <p className='text-xl'>{error}</p>

    if (board === undefined) {
        return <div className='h-dvh bg-sky-600'></div>
    }

    return (
        <div className="relative h-dvh p-8 bg-sky-600 overflow-hidden">
            <div className='flex flex-col h-full px-4'>
                <header className='flex mb-16 w-full justify-center items-center text-center'>
                    <input
                        className='ml-32 text-5xl text-white font-bold bg-transparent'
                        value={board.title}
                        maxLength={32}
                        onChange={(e) => updateTitle(e.target.value)}
                    />
                </header>
                <div className="flex h-full overflow-x-auto">
                    <ControlBar boardId={id} className="absolute ml-8 top-24" />
                    <div className='flex h-full overflow-x-auto'>
                        {board.columns.map((c, i) => {
                            return (
                                <ColumnC ctx={ctx} key={i} column={c} isFirst={i === 0} />
                            )
                        })}
                        <div onClick={addColumn} className='flex justify-center items-center p-2 h-16 min-w-[360px] bg-gray-300 rounded-lg border-2 border-gray-500 shadow-md cursor-pointer hover:border-orange-400'>
                            <p className='text-xl text-gray-600 font-semibold'>+ New</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}

const assertFailed = () => {
    console.trace("Assertion failed");
}

type DragTodo = {
    columnId: string
    todoId: string
}

type ColumnProps = {
    ctx: Context
    column: ColumnDetail
    isFirst: boolean
}

const ColumnC = ({ ctx, column, isFirst }: ColumnProps) => {
    const db = useDB();

    const [titleInFocus, setTitleInFocus] = useState(false);
    const [isDraggingOverBottomSpace, setIsDraggingOverBottomSpace] = useState(false);

    const dragIs = (wanted: 'column' | 'todo') => {
        return ctx.currentDragType === wanted;
    }

    const onDropBottomSpace = (e: DragEvent) => {
        e.preventDefault();
        setIsDraggingOverBottomSpace(false);
        if (dragIs('column')) return;

        ctx.onTodoDropBottomSpace(column.id);
    }

    const allowDroppingBottomSpace = (e: DragEvent) => {
        if (dragIs('column')) return; // disallow
        e.preventDefault();
    }

    const onDragLeaveBottomSpace = (e: DragEvent) => setIsDraggingOverBottomSpace(false);

    const onDragEndBottomSpace = (e: DragEvent) => {
        setIsDraggingOverBottomSpace(false);
        ctx.setCurrentDragType('none');
    }
    const onDragEnterBottomSpace = (e: DragEvent) => {
        if (dragIs('column')) return;
        setIsDraggingOverBottomSpace(true);
    }

    const onColumnDrag = (e: DragEvent) => {
        if (e.dataTransfer?.getData('drag-type') === 'todo') return;
        ctx.setCurrentDragType('column');
        ctx.onColumnDrag(column.id, e);
    }

    const removeColumn = (id: string) => {
        BoardRepo.deleteColumn(db, id);
    }

    const addTodo = () => {
        const todo = {
            id: `${crypto.randomUUID()}`,
            board_id: ctx.boardId,
            column_id: column.id,
            title: "",
            description: "",
            position: "1",
            updated_at: (new Date()).toISOString()
        } as Todo

        BoardRepo.saveTodo(db, todo);
        ctx.autoFocus.setId(todo.id);
    }

    const putCursorAtEndOfLine = (e: Event) => {
        if (e === undefined) return;
        const t = e.target.value;
        e.target.value = '';
        e.target.value = t
    }

    const onFocusTitle = (e: FocusEvent) => {
        putCursorAtEndOfLine(e);
        setTitleInFocus(true);
        ctx.autoFocus.setId(column.id);
    }

    const onClickTitle = () => {
        setTitleInFocus(true);
        ctx.autoFocus.setId(column.id);
    }

    const onBlurTitle = () => {
        setTitleInFocus(false);
        if (column.title === "") {
            removeColumn(column.id);
            return;
        }
    }

    const updateTitle = (title: string) => {
        BoardRepo.saveColumn(db, { ...column, title });
    }

    return (
        <>
            {isFirst && (
                <ColumnSpacer ctx={ctx} columnId={"-1"} />
            )}
            <div draggable onDragStart={onColumnDrag} className='flex flex-col shrink-0 w-[360px] h-full p-2 bg-gray-300 rounded-lg border-2 border-gray-500 shadow-md'>
                <header className='flex justify-between items-center'>
                    {titleInFocus || column.title === "" ?
                        <input
                            className='flex w-full p-2 h-8 overflow-hidden resize-none text-2xl text-gray-600 focus:outline-orange-600 rounded-md'
                            onBlur={onBlurTitle}
                            onChange={e => updateTitle(e.target.value)}
                            onFocus={onFocusTitle}
                            autoFocus={column.id === ctx.autoFocus.currentId}
                            value={column.title}
                        />
                        :
                        <h1 onClick={onClickTitle} className='pb-2 w-full text-2xl text-gray-600'>{column.title}</h1>
                    }
                    <ColumnMenu columnId={column.id} removeColumn={removeColumn} />

                </header>
                <div className='flex flex-col h-full overflow-y-scroll'>
                    {column.todos.map((todo, i) => {
                        return (
                            <Row key={todo.id} ctx={ctx} columnId={column.id} isFirst={i === 0} todo={todo} />
                        )
                    })}
                    <div className={`flex-grow ${isDraggingOverBottomSpace ? 'mb-2 border-2 border-orange-400 rounded-md' : ''}`} onDoubleClick={addTodo} onDrop={onDropBottomSpace} onDragOver={allowDroppingBottomSpace} onDragEnter={onDragEnterBottomSpace} onDragLeave={onDragLeaveBottomSpace} onDragEnd={onDragEndBottomSpace}>
                    </div>
                </div>
                <div onClick={addTodo} className='flex justify-center bg-orange-400 hover:bg-orange-500 cursor-pointer rounded-md'>
                    <p className='py-4 text-white font-semibold'>+ Add</p>
                </div>
            </div>
            <ColumnSpacer ctx={ctx} columnId={column.id} />
        </>
    )
}

type ColumnMenuProps = {
    columnId: string
    removeColumn: (id: string) => void
}

const ColumnMenu = ({ columnId, removeColumn }: ColumnMenuProps) => {
    const [menuIsOpen, setMenuIsOpen] = useState(false);

    const { MoreHorizontal, Trashcan } = useIcon();

    const remove = () => {
        setMenuIsOpen(false);
        removeColumn(columnId);
    }

    return (
        <div className='relative' tabIndex={0} onBlur={() => setMenuIsOpen(false)}>
            <MoreHorizontal onClick={() => setMenuIsOpen(!menuIsOpen)} className='fill-gray-500 w-8 h-8 hover:fill-gray-700 cursor-pointer' />
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

type RowProps = {
    ctx: Context
    columnId: string
    index: number
    todo: Todo
}

const Row = ({ ctx, columnId, isFirst, todo }: RowProps) => {
    const db = useDB();

    const onDragStart = (e: DragEvent) => {
        e.dataTransfer?.setData('drag-type', 'todo'); // NOTE: Along with setting a property on the event we also need to keep a state variable in React because the DragEvent is not propagated to onDragEnter sigh...
        ctx.setCurrentDragType('todo');
        ctx.onTodoDrag(columnId, todo.id, e);
    }

    const deleteIfEmpty = () => {
        if (todo.title === "") {
            BoardRepo.deleteTodo(db, todo.id);
            return;
        }
    };

    const updateTitle = async (title: string) => {
        // NOTE: Desperately this needs something like PerriText and change collapsing
        //       Right now, every keystroke is a save to the database
        BoardRepo.saveTodo(db, { ...todo, title });
    }

    return (
        <>
            {isFirst && (
                <TodoSpacer ctx={ctx} columnId={columnId} todoId={"-1"} />
            )}
            <div draggable onDragStart={onDragStart}>
                <textarea
                    className='w-full p-2 min-h-20 text-start resize-none cursor-pointer rounded-md bg-gray-100 text-gray-600 border-2 border-gray-400 hover:border-orange-400 outline-none focus:border-orange-600'
                    value={todo.title}
                    name='title'
                    autoFocus={todo.id === ctx.autoFocus.currentId}
                    onBlur={deleteIfEmpty}
                    onChange={(e) => updateTitle(e.target.value)}
                />
            </div>
            <TodoSpacer ctx={ctx} columnId={columnId} todoId={todo.id} />
        </>
    )
}

type ColumnSpacerProps = {
    ctx: Context
    columnId: string;
}

const ColumnSpacer = ({ ctx, columnId }: ColumnSpacerProps) => {
    const [isDraggedOver, setIsDraggedOver] = useState(false);

    const drop = (e: DragEvent) => {
        e.preventDefault();
        ctx.onColumnDrop(columnId);
        setIsDraggedOver(false);
    }

    const onDragOver = (e: DragEvent) => {
        if (ctx.currentDragType === 'column') {
            setIsDraggedOver(true);
            e.preventDefault();
        }
    }

    const onDragLeave = () => setIsDraggedOver(false);
    const onDragEnd = () => setIsDraggedOver(false);

    return (
        <div className={`flex h-full items-center ${isDraggedOver ? 'px-8' : 'px-4'}`} onDrop={drop} onDragOver={onDragOver} onDragLeave={onDragLeave} onDragEnd={onDragEnd}>
            {isDraggedOver && (
                <p className='p-1 bg-orange-400 w-4 h-full rounded-md'></p>
            )}
        </div>
    )
}

type TodoSpacerProps = {
    ctx: Context
    todoId: string;
    columnId: string;
}

const TodoSpacer = ({ ctx, todoId, columnId }: TodoSpacerProps) => {

    const [isDraggedOver, setIsDraggedOver] = useState(false);

    const drop = (e: DragEvent) => {
        e.preventDefault();
        ctx.onTodoDrop(columnId, todoId);
        setIsDraggedOver(false);
    }

    const onDragOver = (e: DragEvent) => {
        if (ctx.currentDragType === 'todo') {
            e.preventDefault();
            setIsDraggedOver(true);
        }
    }

    const onDragLeave = (e: DragEvent) => setIsDraggedOver(false);
    const onDragEnd = (e: DragEvent) => setIsDraggedOver(false);

    return (
        <div className={`flex flex-col items-center ${isDraggedOver ? 'py-2' : 'py-1'}`} onDrop={drop} onDragOver={onDragOver} onDragLeave={onDragLeave} onDragEnd={onDragEnd}>
            {isDraggedOver && (
                <p className='p-1 bg-orange-400 w-full rounded-md'></p>
            )}
        </div>
    )
}