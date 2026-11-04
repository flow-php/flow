import Prism from "prismjs"
import "prismjs/components/prism-markup-templating.min.js"
import "prismjs/components/prism-php.min.js"
import "../prismjs/themes/prism-flow.css"
import { EditorView, hoverTooltip, showTooltip } from "@codemirror/view"
import { StateField } from "@codemirror/state"
import {
    activeParameterIndex,
    documentationUrl,
    enclosingCall,
    parseImports,
    shortClassName,
    symbolAt,
    tokenize
} from "./php_analysis.js"

function parameterText(parameter) {
    const type = parameter.type ? `${parameter.type} ` : ""
    const variadic = parameter.variadic ? "..." : ""
    const value = parameter.default !== undefined ? ` = ${parameter.default}` : ""

    return `${type}${variadic}$${parameter.name}${value}`
}

function highlighted(code) {
    const span = document.createElement("span")
    span.innerHTML = Prism.highlight(code, Prism.languages.php, "php")

    return span
}

function signatureElement(entry, activeIndex = -1) {
    const code = document.createElement("div")
    code.style.fontFamily = "'Fira Code', 'JetBrains Mono', monospace"
    code.append(highlighted(`${entry.name}(`))

    entry.parameters.forEach((parameter, index) => {
        if (index > 0) code.append(", ")
        const span = highlighted(parameterText(parameter))
        if (index === activeIndex) span.className = "cm-flow-active-parameter"
        code.append(span)
    })

    const returns = entry.returns.map(shortClassName).join("|")
    code.append(highlighted(`)${returns ? ` : ${returns}` : ""}`))

    return code
}

function textElement(text, styles = {}) {
    const element = document.createElement("div")
    Object.assign(element.style, styles)
    element.textContent = text

    return element
}

function documentationLink(url) {
    const link = document.createElement("a")
    link.href = url
    link.target = "_blank"
    link.className = "cm-flow-documentation-link"
    link.textContent = "Documentation (Cmd/Ctrl + click)"
    link.style.display = "inline-block"
    link.style.marginTop = "8px"

    return link
}

export function infoElement(entry, heading = null, url = entry.documentation) {
    const root = document.createElement("div")

    if (heading) root.append(textElement(heading, { color: "#8b949e", fontSize: "12px", marginBottom: "4px" }))

    const signature = signatureElement(entry)
    signature.style.marginBottom = "8px"
    root.append(signature)

    if (entry.doc) root.append(textElement(entry.doc, { color: "#8b949e", fontSize: "13px", whiteSpace: "pre-line" }))

    if (url) root.append(documentationLink(url))

    return root
}

function withDocumentationLink(element, url) {
    if (url) element.append(documentationLink(url))

    return element
}

function symbolElement(symbol) {
    const url = documentationUrl(symbol)

    switch (symbol.kind) {
        case "function":
            return infoElement(symbol.entry, symbol.entry.namespace, url)
        case "method":
            return infoElement(symbol.entry, symbol.owner, url)
        case "constant":
            return withDocumentationLink(textElement(`${symbol.owner}::${symbol.name}`, { fontFamily: "'Fira Code', 'JetBrains Mono', monospace" }), url)
        case "class":
            return withDocumentationLink(textElement(symbol.fqn, { fontFamily: "'Fira Code', 'JetBrains Mono', monospace" }), url)
        case "variable":
            return symbol.types.length > 0
                ? textElement(`${symbol.name}: ${symbol.types.join("|")}`, { fontFamily: "'Fira Code', 'JetBrains Mono', monospace" })
                : null
    }

    return null
}

export const flowHover = hoverTooltip((view, pos) => {
    const symbol = symbolAt(view.state, pos)
    const dom = symbol ? symbolElement(symbol) : null

    if (!dom) return null

    return { pos: symbol.from, end: symbol.to, above: true, create: () => ({ dom }) }
})

function signatureTooltip(state) {
    const selection = state.selection.main

    if (!selection.empty) return null

    const { tokens, unterminated } = tokenize(state.sliceDoc(0, selection.head))

    if (unterminated === "comment") return null

    const call = enclosingCall(tokens, parseImports(tokenize(state.doc.toString()).tokens))

    if (!call || call.entry.parameters.length === 0) return null

    return {
        pos: call.open.start,
        above: true,
        strictSide: true,
        create: () => {
            const dom = signatureElement(call.entry, activeParameterIndex(call))
            dom.classList.add("cm-flow-signature")
            return { dom }
        }
    }
}

export const flowSignatureHints = StateField.define({
    create: signatureTooltip,
    update: (tooltip, transaction) => transaction.docChanged || transaction.selection
        ? signatureTooltip(transaction.state)
        : tooltip,
    provide: field => showTooltip.from(field)
})

export const flowDocumentationLinks = EditorView.domEventHandlers({
    mousedown(event, view) {
        if (!(event.metaKey || event.ctrlKey)) return false

        const pos = view.posAtCoords({ x: event.clientX, y: event.clientY })
        const documentation = pos === null ? null : documentationUrl(symbolAt(view.state, pos))

        if (!documentation) return false

        event.preventDefault()
        window.open(documentation, "_blank")

        return true
    }
})

export const flowTooltips = [flowHover, flowSignatureHints, flowDocumentationLinks]
