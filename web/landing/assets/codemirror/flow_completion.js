import { pickedCompletion } from "@codemirror/autocomplete"
import { classes, functions } from "./completions/flow-api.js"
import {
    MEMBER_ACCESS,
    documentImports,
    documentationUrl,
    enclosingCall,
    insideUseStatement,
    membersOf,
    parseImports,
    resolveClass,
    shortClassName,
    tokenize,
    typesOfExpression,
    typesOfVariable
} from "./php_analysis.js"
import { infoElement } from "./flow_tooltips.js"

const NAME_DECLARING_KEYWORDS = new Set([
    "function", "fn", "class", "interface", "trait", "enum", "use", "namespace", "const", "insteadof", "as", "goto"
])
const CLASS_EXPECTING_KEYWORDS = new Set(["new", "instanceof", "extends", "implements"])

function callInsertion(state, entry, name, from, to) {
    if (state.sliceDoc(to, to + 1) === "(") return { insert: name, cursor: name.length }

    return { insert: `${name}()`, cursor: name.length + (entry.parameters.length > 0 ? 1 : 2) }
}

function applyMethod(method) {
    return (view, completion, from, to) => {
        const call = callInsertion(view.state, method, method.name, from, to)

        view.dispatch({
            changes: { from, to, insert: call.insert },
            selection: { anchor: from + call.cursor },
            annotations: pickedCompletion.of(completion),
            userEvent: "input.complete"
        })
    }
}

function importPlan(doc, kind, fqn) {
    const name = fqn.split("\\").pop()
    const namespace = fqn.slice(0, -name.length - 1)
    const keyword = kind === "function" ? "use function" : "use"
    const { tokens } = tokenize(doc)
    const imports = parseImports(tokens)
    const imported = (kind === "function" ? imports.functions : imports.classes).get(name.toLowerCase())

    if (imported) {
        return { name: imported.toLowerCase() === fqn.toLowerCase() ? name : `\\${fqn}`, change: null }
    }

    const group = imports.statements.find(statement =>
        statement.kind === kind
        && statement.group?.close
        && statement.group.prefix.toLowerCase() === namespace.toLowerCase()
        && statement.group.entries.length > 0
    )?.group

    if (group) return { name, change: groupInsertion(doc, group, name) }

    const sameKind = imports.statements.filter(statement => statement.kind === kind).pop()

    if (sameKind) return { name, change: { from: sameKind.end, insert: `\n${keyword} ${fqn};` } }

    const firstOther = imports.statements[0]

    if (firstOther && kind === "class") return { name, change: { from: firstOther.start, insert: `${keyword} ${fqn};\n` } }

    const lastOther = imports.statements[imports.statements.length - 1]

    if (lastOther) return { name, change: { from: lastOther.end, insert: `\n${keyword} ${fqn};` } }

    const statementAnchor = imports.anchors.find(candidate => candidate.kind === "namespace") ?? imports.anchors.find(candidate => candidate.kind === "declare")
    const position = statementAnchor?.end ?? tokens.find(token => token.type === "open")?.end

    if (position === undefined) return { name: `\\${fqn}`, change: null }

    return { name, change: { from: position, insert: `\n\n${keyword} ${fqn};` } }
}

function groupInsertion(doc, group, name) {
    const entries = group.entries
    const separator = entries.length > 1
        ? doc.slice(entries[0].end, entries[1].start)
        : doc.slice(group.open.end, entries[0].start).includes("\n")
            ? `,${doc.slice(group.open.end, entries[0].start)}`
            : ", "
    const next = entries.find(entry => entry.name.toLowerCase() > name.toLowerCase())

    if (next) return { from: next.start, insert: `${name}${separator}` }

    return { from: entries[entries.length - 1].end, insert: `${separator}${name}` }
}

function applyImported(kind, fqn, entry) {
    return (view, completion, from, to) => {
        const plan = importPlan(view.state.doc.toString(), kind, fqn)
        const call = entry ? callInsertion(view.state, entry, plan.name, from, to) : { insert: plan.name, cursor: plan.name.length }
        const changes = view.state.changes([
            ...(plan.change ? [plan.change] : []),
            { from, to, insert: call.insert }
        ])

        view.dispatch({
            changes,
            selection: { anchor: changes.mapPos(from, -1) + call.cursor },
            annotations: pickedCompletion.of(completion),
            userEvent: "input.complete",
            scrollIntoView: true
        })
    }
}

function namespaceBoost(namespace) {
    if (namespace === "Flow\\ETL\\DSL") return 3
    return namespace.startsWith("Flow\\ETL\\") ? 2 : 0
}

const functionOptions = functions.map(fn => ({
    label: fn.name,
    type: "function",
    detail: fn.namespace,
    info: () => infoElement(fn),
    apply: applyImported("function", `${fn.namespace}\\${fn.name}`, fn),
    boost: namespaceBoost(fn.namespace)
}))

const classOptions = Object.keys(classes).map(fqn => {
    const namespace = fqn.slice(0, -shortClassName(fqn).length - 1)

    return {
        label: shortClassName(fqn),
        type: "class",
        detail: namespace,
        info: fqn,
        apply: applyImported("class", fqn, null),
        boost: namespaceBoost(namespace)
    }
})

const wordOptions = [...functionOptions, ...classOptions]

function methodOptions(receivers) {
    const options = new Map()

    for (const receiver of receivers) {
        for (const [key, { member: method, owner }] of membersOf(receiver, "methods")) {
            if (options.has(key)) continue
            options.set(key, {
                label: method.name,
                type: "method",
                detail: shortClassName(owner),
                info: () => infoElement(method, null, documentationUrl({ kind: "method", entry: method, owner })),
                apply: applyMethod(method)
            })
        }
    }

    return [...options.values()]
}

function staticOptions(fqn) {
    const constants = [...membersOf(fqn, "constants").values()].map(({ member, owner }) => ({
        label: member,
        type: "constant",
        detail: shortClassName(owner)
    }))
    const staticMethods = [...membersOf(fqn, "staticMethods").values()].map(({ member, owner }) => ({
        label: member.name,
        type: "method",
        detail: shortClassName(owner),
        info: () => infoElement(member, null, documentationUrl({ kind: "method", entry: member, owner })),
        apply: applyMethod(member)
    }))

    return [{ label: "class", type: "keyword", detail: fqn }, ...constants, ...staticMethods]
}

function namedArgumentOptions(call) {
    return call.entry.parameters
        .filter(parameter => !parameter.variadic && !call.usedNames.has(parameter.name))
        .map(parameter => ({
            label: `${parameter.name}:`,
            type: "property",
            detail: parameter.type,
            apply: `${parameter.name}: `,
            boost: 10
        }))
}

function variableOptions(tokens, typedIndex, imports) {
    const options = new Map()

    tokens.forEach((token, index) => {
        if (token.type !== "variable" || index === typedIndex || token.value === "$this" || options.has(token.value)) return

        const types = typesOfVariable(tokens, token.value, typedIndex, imports)
        options.set(token.value, {
            label: token.value,
            type: "variable",
            detail: types.map(shortClassName).join("|") || undefined
        })
    })

    return [...options.values()]
}

export function flowCompletion(context) {
    const { tokens, unterminated } = tokenize(context.state.sliceDoc(0, context.pos))

    if (unterminated) return null

    let last = tokens.length - 1
    let from = context.pos
    let typed = ""

    if (["variable", "operator"].includes(tokens[last]?.type) && tokens[last].value.startsWith("$") && tokens[last].end === context.pos) {
        return { from: tokens[last].start, options: variableOptions(tokens, last, documentImports(context.state)), validFor: /^\$\w*$/ }
    }

    if (tokens[last]?.type === "name" && tokens[last].end === context.pos) {
        typed = tokens[last].value
        from = tokens[last].start
        last--
    }

    if (typed.includes("\\")) return null

    const before = tokens[last]

    if (before && MEMBER_ACCESS.has(before.value)) {
        const receivers = typesOfExpression(tokens, last - 1, documentImports(context.state))

        return receivers.length > 0 ? { from, options: methodOptions(receivers), validFor: /^\w*$/ } : null
    }

    if (before?.value === "::") {
        const owner = tokens[last - 1]
        const fqn = owner?.type === "name" ? resolveClass(owner.value, documentImports(context.state)) : null

        return fqn ? { from, options: staticOptions(fqn), validFor: /^\w*$/ } : null
    }

    if ((typed === "" && !context.explicit) || insideUseStatement(tokens, last)) return null

    const keyword = before?.type === "name" ? before.value.toLowerCase() : null

    if (NAME_DECLARING_KEYWORDS.has(keyword)) return null

    if (CLASS_EXPECTING_KEYWORDS.has(keyword)) return { from, options: classOptions, validFor: /^\w*$/ }

    const startsArgument = before && ["(", ","].includes(before.value)
    const call = startsArgument ? enclosingCall(tokens.slice(0, last + 1), documentImports(context.state)) : null

    return { from, options: [...(call ? namedArgumentOptions(call) : []), ...wordOptions], validFor: /^\w*$/ }
}
