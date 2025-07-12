export type SelectedItems = {
    type: 'table' | 'row'
    items: (string | number)[]
} | undefined

export type RightClickTableEvent = {
    tableName: string
    mouseX: number
    mouseY: number
} | undefined

export type RightClickDataEvent = {
    sql: string
    mouseX: number
    mouseY: number
}