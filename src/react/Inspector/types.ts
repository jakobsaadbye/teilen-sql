export type SelectedItems = {
    type: 'table' | 'row'
    items: number[]
} | undefined

export type RightClickTableEvent = {
    tableIndex: number
    mouseX: number
    mouseY: number
} | undefined

export type RightClickDataEvent = {
    sql: string
    mouseX: number
    mouseY: number
}