export type Todo = {
    id: string
    board_id: string
    column_id: string
    title: string
    description: string
    position: string
    updated_at: string
}

export type Column = {
    id: string
    board_id: string
    title: string
    position: string
}

export type Board = {
    id: string
    title: string
    created_at: string
    updated_at: string
}

export type ColumnDetail = Column & {
    todos: Todo[]
} 

export type BoardDetail = Board & {
    columns: ColumnDetail[]
}