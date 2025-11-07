import { EditorView } from "@codemirror/view"
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language"
import { tags as t } from "@lezer/highlight"

export const flowTheme = EditorView.theme({
    "&": {
        backgroundColor: "#1b1925",
        color: "#f8f8f2",
        height: "100%"
    },
    ".cm-content": {
        fontSize: "16px",
        lineHeight: "1.4",
        caretColor: "#f8f8f2"
    },
    ".cm-cursor, .cm-dropCursor": {
        borderLeftColor: "#f8f8f2"
    },
    ".cm-selectionBackground, ::selection": {
        backgroundColor: "rgba(121, 192, 255, 0.2)"
    },
    "&.cm-focused .cm-selectionBackground": {
        backgroundColor: "rgba(121, 192, 255, 0.2)"
    },
    ".cm-activeLine": {
        backgroundColor: "rgba(255, 255, 255, 0.05)"
    },
    ".cm-gutters": {
        backgroundColor: "#1b1925",
        color: "#8b949e",
        border: "none"
    },
    ".cm-activeLineGutter": {
        backgroundColor: "rgba(255, 255, 255, 0.05)"
    },
    ".cm-foldPlaceholder": {
        backgroundColor: "#ff7b72",
        border: "none",
        color: "#f8f8f2"
    },
    ".cm-searchMatch": {
        backgroundColor: "rgba(121, 192, 255, 0.4)"
    },
    ".cm-searchMatch.cm-searchMatch-selected": {
        backgroundColor: "rgba(121, 192, 255, 0.6)"
    },
    ".cm-error": {
        backgroundColor: "rgba(255, 123, 114, 0.15)",
        borderLeft: "3px solid #ff7b72"
    },
    ".cm-tooltip.cm-tooltip-autocomplete": {
        backgroundColor: "#21212b",
        border: "2px solid #79c0ff",
        borderRadius: "8px",
        boxShadow: "0 8px 32px rgba(0, 0, 0, 0.5)",
        fontFamily: "'Cabin Variable', system-ui",
        fontSize: "16px",
        padding: "6px"
    },
    ".cm-tooltip-autocomplete ul": {
        fontFamily: "inherit",
        maxHeight: "400px"
    },
    ".cm-tooltip-autocomplete ul li": {
        padding: "8px 14px",
        borderRadius: "5px",
        margin: "2px 0",
        color: "#f8f8f2",
        cursor: "pointer"
    },
    ".cm-tooltip-autocomplete ul li[aria-selected]": {
        background: "linear-gradient(90deg, #79c0ff 0%, #6bb0f0 100%)",
        color: "#0d1117",
        fontWeight: "600",
        boxShadow: "0 2px 8px rgba(121, 192, 255, 0.3)"
    },
    ".cm-completionLabel": {
        color: "inherit"
    },
    ".cm-completionDetail": {
        color: "#8b949e",
        fontSize: "12px",
        fontStyle: "italic",
        marginLeft: "16px"
    },
    ".cm-tooltip-autocomplete ul li[aria-selected] .cm-completionDetail": {
        color: "#0d1117",
        fontWeight: "500"
    },
    ".cm-completionIcon": {
        fontSize: "14px",
        width: "1em",
        lineHeight: "1",
        marginRight: "8px",
        textAlign: "center",
        paddingRight: "4px"
    },
    ".cm-completionIcon-function": { color: "#d2a8ff" },
    ".cm-completionIcon-class": { color: "#79c0ff" },
    ".cm-completionIcon-keyword": { color: "#ff7b72" },
    ".cm-completionIcon-variable": { color: "#f8f8f2" },
    ".cm-completionIcon-constant": { color: "#79c0ff" },
    ".cm-completionIcon-type": { color: "#79c0ff" },
    ".cm-completionIcon-namespace": { color: "#d2a8ff" },
    ".cm-tooltip.cm-completionInfo": {
        backgroundColor: "#21212b",
        border: "2px solid #79c0ff",
        borderRadius: "8px",
        boxShadow: "0 8px 32px rgba(0, 0, 0, 0.5)",
        color: "#f8f8f2",
        fontFamily: "'Cabin Variable', system-ui",
        fontSize: "14px",
        maxWidth: "600px",
        padding: "16px"
    },
    ".cm-completionInfo code": {
        backgroundColor: "#1b1925",
        padding: "2px 6px",
        borderRadius: "4px",
        fontFamily: "'Fira Code', 'JetBrains Mono', 'Consolas', monospace",
        fontSize: "13px"
    }
}, { dark: true })

export const flowHighlightStyle = HighlightStyle.define([
    { tag: t.keyword, color: "#ff7b72" },
    { tag: [t.name, t.deleted, t.character, t.propertyName, t.macroName], color: "#f8f8f2" },
    { tag: [t.function(t.variableName), t.labelName], color: "#d2a8ff" },
    { tag: [t.color, t.constant(t.name), t.standard(t.name)], color: "#79c0ff" },
    { tag: [t.definition(t.name), t.separator], color: "#f8f8f2" },
    { tag: [t.typeName, t.className, t.number, t.changed, t.annotation, t.modifier, t.self, t.namespace], color: "#79c0ff" },
    { tag: [t.operator, t.operatorKeyword, t.url, t.escape, t.regexp, t.link, t.special(t.string)], color: "#79c0ff" },
    { tag: [t.meta, t.comment], color: "#8292a2" },
    { tag: t.strong, fontWeight: "bold" },
    { tag: t.emphasis, fontStyle: "italic" },
    { tag: t.strikethrough, textDecoration: "line-through" },
    { tag: t.link, color: "#79c0ff", textDecoration: "underline" },
    { tag: t.heading, fontWeight: "bold", color: "#79c0ff" },
    { tag: [t.atom, t.bool, t.special(t.variableName)], color: "#79c0ff" },
    { tag: [t.processingInstruction, t.string, t.inserted], color: "#79c0ff" },
    { tag: t.invalid, color: "#f8f8f2", backgroundColor: "#ff7b72" }
])

export const flowThemeExtension = [
    flowTheme,
    syntaxHighlighting(flowHighlightStyle)
]
