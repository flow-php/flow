import { classes, functions } from "./completions/flow-api.js"

const OPERATORS = [
    "?->", "->", "::", "|>", "=>", "...", "===", "!==", "<=>", "??=", "==", "!=", "<=", ">=",
    "&&", "||", "??", ".=", "+=", "-=", "*=", "/=", "++", "--"
]
const LANGUAGE_CONSTRUCTS = new Set([
    "return", "echo", "print", "yield", "throw", "clone", "and", "or", "xor", "case", "else",
    "include", "include_once", "require", "require_once", "instanceof"
])
export const MEMBER_ACCESS = new Set(["->", "?->"])
const OPENING = new Set(["(", "[", "{"])
const CLOSING = new Set([")", "]", "}"])
const MAX_RESOLVE_DEPTH = 64

const functionsByFqn = new Map()
const functionsByName = new Map()
const classesByFqn = new Map()
const classesByShortName = new Map()
const membersCache = new Map()

export function functionFqn(fn) {
    return fn.namespace ? `${fn.namespace}\\${fn.name}` : fn.name
}

for (const fn of functions) {
    functionsByFqn.set(functionFqn(fn).toLowerCase(), fn)
    const named = functionsByName.get(fn.name.toLowerCase()) ?? []
    named.push(fn)
    functionsByName.set(fn.name.toLowerCase(), named)
}

for (const fqn of Object.keys(classes)) {
    classesByFqn.set(fqn.toLowerCase(), fqn)
    const shortName = shortClassName(fqn).toLowerCase()
    classesByShortName.set(shortName, [...(classesByShortName.get(shortName) ?? []), fqn])
}

export function tokenize(text) {
    const tokens = []
    const length = text.length
    const openTag = text.indexOf("<?php")
    let i = 0

    if (openTag !== -1) {
        tokens.push({ type: "open", value: "<?php", start: openTag, end: openTag + 5 })
        i = openTag + 5
    }

    while (i < length) {
        const char = text[i]

        if (/\s/.test(char)) {
            i++
            continue
        }

        if ((char === "/" && text[i + 1] === "/") || (char === "#" && text[i + 1] !== "[")) {
            const end = text.indexOf("\n", i)
            if (end === -1) return { tokens, unterminated: "comment" }
            i = end + 1
            continue
        }

        if (char === "/" && text[i + 1] === "*") {
            const end = text.indexOf("*/", i + 2)
            if (end === -1) return { tokens, unterminated: "comment" }
            i = end + 2
            continue
        }

        if (char === "'" || char === '"' || char === "`") {
            let j = i + 1
            while (j < length && text[j] !== char) {
                j += text[j] === "\\" ? 2 : 1
            }
            if (j >= length) return { tokens, unterminated: "string" }
            tokens.push({ type: "string", value: text.slice(i, j + 1), start: i, end: j + 1 })
            i = j + 1
            continue
        }

        const heredoc = text.startsWith("<<<", i) ? /^<<<[ \t]*(['"]?)([A-Za-z_]\w*)\1\r?\n/.exec(text.slice(i)) : null
        if (heredoc) {
            const bodyStart = i + heredoc[0].length
            const closing = new RegExp(`^[ \\t]*${heredoc[2]}\\b`, "m").exec(text.slice(bodyStart))
            if (!closing) return { tokens, unterminated: "string" }
            const end = bodyStart + closing.index + closing[0].length
            tokens.push({ type: "string", value: text.slice(i, end), start: i, end })
            i = end
            continue
        }

        const variable = char === "$" ? /^\$[A-Za-z_\x80-￿][\w\x80-￿]*/.exec(text.slice(i, i + 256)) : null
        if (variable) {
            tokens.push({ type: "variable", value: variable[0], start: i, end: i + variable[0].length })
            i += variable[0].length
            continue
        }

        const name = /^\\?[A-Za-z_\x80-￿][\w\x80-￿]*(?:\\[A-Za-z_\x80-￿][\w\x80-￿]*)*\\?/.exec(text.slice(i, i + 512))
        if (name) {
            tokens.push({ type: "name", value: name[0], start: i, end: i + name[0].length })
            i += name[0].length
            continue
        }

        const number = /^\d[\w.]*/.exec(text.slice(i, i + 64))
        if (number) {
            tokens.push({ type: "number", value: number[0], start: i, end: i + number[0].length })
            i += number[0].length
            continue
        }

        const operator = OPERATORS.find(candidate => text.startsWith(candidate, i)) ?? char
        tokens.push({ type: "operator", value: operator, start: i, end: i + operator.length })
        i += operator.length
    }

    return { tokens, unterminated: null }
}

export function parseImports(tokens) {
    const imports = { statements: [], functions: new Map(), classes: new Map(), anchors: [] }

    for (let i = 0; i < tokens.length; i++) {
        const token = tokens[i]
        const previous = tokens[i - 1]
        const startsStatement = !previous || previous.type === "open" || [";", "{", "}"].includes(previous.value)

        if (token.type !== "name" || !startsStatement) continue

        const keyword = token.value.toLowerCase()

        if (keyword === "declare" || keyword === "namespace") {
            const end = tokens.findIndex((candidate, index) => index > i && (candidate.value === ";" || candidate.value === "{"))
            if (end !== -1 && tokens[end].value === ";") imports.anchors.push({ kind: keyword, end: tokens[end].end })
            continue
        }

        if (keyword !== "use") continue

        const statement = parseUseStatement(tokens, i)
        if (!statement) continue

        imports.statements.push(statement)
        const target = statement.kind === "function" ? imports.functions : statement.kind === "class" ? imports.classes : null
        for (const entry of statement.entries) {
            target?.set(entry.alias.toLowerCase(), entry.fqn)
        }
        i = statement.endIndex
    }

    return imports
}

function parseUseStatement(tokens, useIndex) {
    let i = useIndex + 1
    let kind = "class"

    if (tokens[i]?.type === "name" && ["function", "const"].includes(tokens[i].value.toLowerCase())) {
        kind = tokens[i].value.toLowerCase()
        i++
    }

    const entries = []
    let group = null

    while (i < tokens.length && tokens[i].value !== ";") {
        const token = tokens[i]

        if (token.type === "name" && tokens[i + 1]?.value === "{") {
            group = { prefix: trimSlashes(token.value), entries: [], open: tokens[i + 1], close: null }
            i += 2
            continue
        }

        if (token.type === "name") {
            const hasAlias = tokens[i + 1]?.value.toLowerCase() === "as" && tokens[i + 2]?.type === "name"
            const path = trimSlashes(token.value)
            const fqn = group ? `${group.prefix}\\${path}` : path
            const entry = {
                fqn,
                alias: hasAlias ? tokens[i + 2].value : path.split("\\").pop(),
                name: path,
                start: token.start,
                end: hasAlias ? tokens[i + 2].end : token.end
            }
            entries.push(entry)
            group?.entries.push(entry)
            i += hasAlias ? 3 : 1
            continue
        }

        if (token.value === "}" && group) group.close = token

        i++
    }

    if (i >= tokens.length) return null

    return { kind, entries, group, start: tokens[useIndex].start, end: tokens[i].end, endIndex: i }
}

function trimSlashes(name) {
    return name.replace(/^\\+|\\+$/g, "")
}

export function shortClassName(fqn) {
    return fqn.split("\\").pop()
}

export function resolveFunction(name, imports) {
    if (name.includes("\\")) return functionsByFqn.get(trimSlashes(name).toLowerCase()) ?? null

    const imported = imports.functions.get(name.toLowerCase())
    if (imported) return functionsByFqn.get(imported.toLowerCase()) ?? null

    const candidates = functionsByName.get(name.toLowerCase()) ?? []
    return candidates.find(fn => fn.namespace === "")
        ?? candidates.find(fn => fn.namespace === "Flow\\ETL\\DSL")
        ?? (candidates.length === 1 ? candidates[0] : null)
}

export function resolveClass(name, imports) {
    if (name.startsWith("\\")) return classesByFqn.get(trimSlashes(name).toLowerCase()) ?? null

    const [first, ...rest] = name.split("\\")
    const imported = imports.classes.get(first.toLowerCase())
    if (imported) return classesByFqn.get([imported, ...rest].join("\\").toLowerCase()) ?? null

    const qualified = classesByFqn.get(name.toLowerCase())
    if (qualified) return qualified

    const candidates = classesByShortName.get(name.toLowerCase()) ?? []
    return candidates.length === 1 ? candidates[0] : null
}

export function membersOf(fqn, kind) {
    const cacheKey = `${kind}:${fqn}`
    if (membersCache.has(cacheKey)) return membersCache.get(cacheKey)

    const members = new Map()
    const visited = new Set()
    const queue = [fqn]

    while (queue.length > 0) {
        const current = queue.shift()
        if (visited.has(current) || !classes[current]) continue
        visited.add(current)

        for (const member of classes[current][kind]) {
            const key = (typeof member === "string" ? member : member.name).toLowerCase()
            if (!members.has(key)) members.set(key, { member, owner: current })
        }
        queue.push(...classes[current].supertypes)
    }

    membersCache.set(cacheKey, members)
    return members
}

function returnedClasses(returns, receiver) {
    return returns
        .map(type => ["self", "static"].includes(type.toLowerCase()) ? receiver : type)
        .filter(type => type !== null && classes[type] !== undefined)
}

function unique(values) {
    return [...new Set(values)]
}

function matchingOpenIndex(tokens, closeIndex) {
    let depth = 0

    for (let i = closeIndex; i >= 0; i--) {
        if (CLOSING.has(tokens[i].value)) depth++
        if (OPENING.has(tokens[i].value)) depth--
        if (depth === 0) return i
    }

    return -1
}

function expressionEndIndex(tokens, startIndex) {
    let depth = 0

    for (let i = startIndex; i < tokens.length; i++) {
        if (OPENING.has(tokens[i].value)) depth++
        if (CLOSING.has(tokens[i].value)) depth--
        if (depth < 0 || (depth === 0 && [";", ","].includes(tokens[i].value))) return i
    }

    return -1
}

export function typesOfExpression(tokens, endIndex, imports, depth = 0) {
    const token = tokens[endIndex]

    if (!token || depth > MAX_RESOLVE_DEPTH) return []

    if (token.type === "variable") return typesOfVariable(tokens, token.value, endIndex, imports, depth)

    if (token.type === "name" && tokens[endIndex - 1]?.value.toLowerCase() === "new") {
        const fqn = resolveClass(token.value, imports)
        return fqn ? [fqn] : []
    }

    if (token.value !== ")") return []

    const openIndex = matchingOpenIndex(tokens, endIndex)
    if (openIndex === -1) return []

    const callee = tokens[openIndex - 1]

    if (!callee || callee.type !== "name" || LANGUAGE_CONSTRUCTS.has(callee.value.toLowerCase())) {
        return callee?.type === "variable" || callee?.value === ")" || callee?.value === "]"
            ? []
            : typesOfExpression(tokens, endIndex - 1, imports, depth + 1)
    }

    if (tokens[openIndex - 2]?.type === "name" && tokens[openIndex - 2].value.toLowerCase() === "new") {
        const fqn = resolveClass(callee.value, imports)
        return fqn ? [fqn] : []
    }

    return unique(callTargets(tokens, openIndex - 1, imports, depth + 1)
        .flatMap(target => returnedClasses(target.entry.returns, target.receiver)))
}

export function callTargets(tokens, nameIndex, imports, depth = 0) {
    const callee = tokens[nameIndex]
    const before = tokens[nameIndex - 1]
    const key = callee.value.toLowerCase()

    if (before && MEMBER_ACCESS.has(before.value)) {
        return typesOfExpression(tokens, nameIndex - 2, imports, depth + 1).flatMap(receiver => {
            const found = membersOf(receiver, "methods").get(key)
            return found ? [{ entry: found.member, owner: found.owner, receiver }] : []
        })
    }

    if (before?.value === "::") {
        const fqn = tokens[nameIndex - 2]?.type === "name" ? resolveClass(tokens[nameIndex - 2].value, imports) : null
        const found = fqn ? membersOf(fqn, "staticMethods").get(key) : null
        return found ? [{ entry: found.member, owner: found.owner, receiver: fqn }] : []
    }

    if (before?.type === "name" && ["new", "function", "fn"].includes(before.value.toLowerCase())) return []

    const fn = resolveFunction(callee.value, imports)
    return fn ? [{ entry: fn, owner: null, receiver: null }] : []
}

export function typesOfVariable(tokens, variable, beforeIndex, imports, depth = 0) {
    if (variable === "$this") return []

    for (let i = beforeIndex - 1; i >= 0; i--) {
        if (tokens[i].type !== "variable" || tokens[i].value !== variable) continue

        if (tokens[i + 1]?.value === "=") {
            const end = expressionEndIndex(tokens, i + 2)
            if (end === -1 || end > beforeIndex) continue
            return typesOfExpression(tokens, end - 1, imports, depth + 1)
        }

        const hint = tokens[i - 1]?.value === "..." ? tokens[i - 2] : tokens[i - 1]
        const beforeHint = tokens[tokens.indexOf(hint) - 1]
        if (hint?.type === "name" && ["(", ",", "?"].includes(beforeHint?.value)) {
            const fqn = resolveClass(hint.value, imports)
            if (fqn) return [fqn]
        }
    }

    return []
}

export function insideUseStatement(tokens, index) {
    for (let i = index; i >= 0; i--) {
        const token = tokens[i]

        if (token.type === "open" || token.value === ";" || token.value === "}") return false
        if (token.value === "{" && !tokens[i - 1]?.value.endsWith("\\")) return false
        if (token.type === "name" && token.value.toLowerCase() === "use") return true
    }

    return false
}

export function documentImports(state) {
    return parseImports(tokenize(state.doc.toString()).tokens)
}

export function enclosingCall(tokens, imports) {
    let depth = 0
    let argument = 0
    let argumentStart = null

    for (let i = tokens.length - 1; i >= 0; i--) {
        const value = tokens[i].value

        if (tokens[i].type === "string" || tokens[i].type === "variable") continue

        if (CLOSING.has(value)) {
            depth++
            continue
        }

        if (OPENING.has(value) && depth > 0) {
            depth--
            continue
        }

        if (value === "[" || value === "{") {
            argument = 0
            argumentStart = null
            continue
        }

        if (value === ",") {
            if (depth === 0) {
                argument++
                argumentStart ??= i + 1
            }
            continue
        }

        if (value === ";") return null

        if (value !== "(") continue

        const callee = tokens[i - 1]
        if (callee?.type !== "name" || LANGUAGE_CONSTRUCTS.has(callee.value.toLowerCase())) return null

        const target = callTargets(tokens, i - 1, imports)[0]
        if (!target) return null

        argumentStart ??= i + 1
        const named = tokens[argumentStart]?.type === "name" && tokens[argumentStart + 1]?.value === ":"
            ? tokens[argumentStart].value
            : null

        return { ...target, open: tokens[i], argument, named, usedNames: namedArguments(tokens, i + 1) }
    }

    return null
}

function namedArguments(tokens, start) {
    const names = new Set()
    let depth = 0

    for (let i = start; i < tokens.length; i++) {
        if (OPENING.has(tokens[i].value)) depth++
        if (CLOSING.has(tokens[i].value)) depth--
        if (depth === 0 && tokens[i].type === "name" && tokens[i + 1]?.value === ":" && ["(", ","].includes(tokens[i - 1]?.value)) {
            names.add(tokens[i].value)
        }
    }

    return names
}

export function activeParameterIndex(call) {
    const parameters = call.entry.parameters

    if (call.named) return parameters.findIndex(parameter => parameter.name === call.named)

    const variadic = parameters.findIndex(parameter => parameter.variadic)
    if (variadic !== -1 && call.argument >= variadic) return variadic

    return call.argument < parameters.length ? call.argument : -1
}

export function symbolAt(state, pos) {
    const { tokens } = tokenize(state.doc.toString())
    const index = tokens.findIndex(token => token.start <= pos && pos <= token.end && ["name", "variable"].includes(token.type))

    return index === -1 ? null : symbolAtIndex(tokens, index, parseImports(tokens))
}

export function symbolAtIndex(tokens, index, imports) {
    const token = tokens[index]
    const range = { from: token.start, to: token.end }

    if (token.type === "variable") {
        return { kind: "variable", name: token.value, types: typesOfVariable(tokens, token.value, index, imports), ...range }
    }

    if (insideUseStatement(tokens, index)) return null

    const before = tokens[index - 1]
    const calls = tokens[index + 1]?.value === "("

    if (before && (MEMBER_ACCESS.has(before.value) || (before.value === "::" && calls))) {
        const target = callTargets(tokens, index, imports)[0]
        return target ? { kind: "method", ...target, ...range } : null
    }

    if (before?.value === "::") {
        const fqn = tokens[index - 2]?.type === "name" ? resolveClass(tokens[index - 2].value, imports) : null
        const found = fqn ? membersOf(fqn, "constants").get(token.value.toLowerCase()) : null
        return found ? { kind: "constant", name: found.member, owner: found.owner, ...range } : null
    }

    if (calls && before?.value.toLowerCase() !== "new") {
        const fn = resolveFunction(token.value, imports)
        return fn ? { kind: "function", entry: fn, ...range } : null
    }

    const fqn = resolveClass(token.value, imports)
    return fqn ? { kind: "class", fqn, ...range } : null
}

export function documentationUrl(symbol) {
    switch (symbol?.kind) {
        case "function":
            return symbol.entry.documentation ?? null
        case "method":
            return symbol.entry.documentation
                ?? (classes[symbol.owner]?.api ? `${classes[symbol.owner].api}#method_${symbol.entry.name}` : null)
        case "constant":
            return classes[symbol.owner]?.api
                ? `${classes[symbol.owner].api}#constant_${symbol.name}`
                : classes[symbol.owner]?.documentation ?? null
        case "class":
            return classes[symbol.fqn]?.api ?? classes[symbol.fqn]?.documentation ?? null
    }

    return null
}
