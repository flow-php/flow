import Prism from "prismjs"
import { documentationUrl, parseImports, symbolAtIndex, tokenize } from "../../../codemirror/php_analysis.js"

function linkedRanges(code) {
    const { tokens } = tokenize(code)
    const imports = parseImports(tokens)
    const links = []

    tokens.forEach((token, index) => {
        const referenced = tokens[index + 1]?.value === "(" || tokens[index + 1]?.value === "::" || tokens[index - 1]?.value === "::"

        if (token.type !== "name" || !referenced) return

        const url = documentationUrl(symbolAtIndex(tokens, index, imports))
        if (url) links.push({ from: token.start, to: token.end, url })
    })

    return links
}

function link(url, className) {
    const anchor = document.createElement("a")
    anchor.href = url
    anchor.target = "_blank"
    if (className) anchor.className = className

    return anchor
}

function wrap(root, links) {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT)
    const nodes = []
    let offset = 0

    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        nodes.push({ node, start: offset })
        offset += node.textContent.length
    }

    for (const { from, to, url } of links.reverse()) {
        const entry = nodes.findLast(candidate => candidate.start <= from)
        if (!entry || entry.start + entry.node.textContent.length < to) continue

        const { node, start } = entry
        const parent = node.parentNode

        if (parent !== root && parent.textContent === node.textContent && node.textContent.length === to - from) {
            const anchor = link(url, parent.className)
            parent.replaceWith(anchor)
            anchor.append(node)
            continue
        }

        const text = node.splitText(from - start)
        text.splitText(to - from)
        const anchor = link(url, null)
        text.replaceWith(anchor)
        anchor.append(text)
    }
}

Prism.hooks.add("after-highlight", env => {
    if (env.language !== "php" || env.element.hasAttribute("data-prismjs-no-auto-linker")) return

    wrap(env.element, linkedRanges(env.code))
})
