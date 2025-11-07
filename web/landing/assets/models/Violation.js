export default class Violation {
    #message
    #line
    #column
    #startLine
    #endLine
    #startColumn
    #endColumn

    constructor(message, line, column, startLine, endLine, startColumn, endColumn) {
        this.#message = message
        this.#line = line
        this.#column = column
        this.#startLine = startLine
        this.#endLine = endLine
        this.#startColumn = startColumn
        this.#endColumn = endColumn
    }

    static create(data) {
        return new Violation(
            data.message,
            data.line,
            data.column,
            data.startLine ?? data.line,
            data.endLine ?? data.line,
            data.startColumn ?? 0,
            data.endColumn ?? Number.MAX_SAFE_INTEGER
        )
    }

    get message() {
        return this.#message
    }

    get line() {
        return this.#line
    }

    getStartPosition(doc) {
        const startLine = Math.max(0, this.#startLine - 1)
        const startColumn = Math.max(0, this.#startColumn)
        const lineObj = doc.line(Math.min(startLine + 1, doc.lines))
        return lineObj.from + Math.min(startColumn, lineObj.length)
    }

    getEndPosition(doc) {
        const endLine = Math.max(0, this.#endLine - 1)
        const endColumn = this.#endColumn === Number.MAX_SAFE_INTEGER
            ? Number.MAX_SAFE_INTEGER
            : Math.max(0, this.#endColumn)
        const lineObj = doc.line(Math.min(endLine + 1, doc.lines))

        if (endColumn === Number.MAX_SAFE_INTEGER) {
            return lineObj.to
        }
        return lineObj.from + Math.min(endColumn, lineObj.length)
    }
}
