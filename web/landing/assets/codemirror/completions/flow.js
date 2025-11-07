/**
 * CodeMirror Completer for Flow PHP Flow Methods
 *
 * Flow methods: 5
 * Flow-returning functions: 2
 *
 * This completer triggers after Flow-returning functions: df()->, data_frame()->
 */

import { CompletionContext, snippet } from "@codemirror/autocomplete"

// Map of Flow-returning functions from dsl.json (df, data_frame)
const flowFunctions = [
    "df", "data_frame"]

// Flow methods
const flowMethods = [
        {
        label: "setUp",
        type: "method",
        detail: "Flow\\\\ETL\\\\Flow",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">setUp</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ConfigBuilder|Config</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("setUp(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },        {
        label: "extract",
        type: "method",
        detail: "Flow\\\\ETL\\\\Flow",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">extract</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrame</span>
                </div>
                            `
            return div
        },
        apply: snippet("extract(" + "$" + "{" + "1:extractor" + "}" + ")"),
        boost: 10
    },        {
        label: "from",
        type: "method",
        detail: "Flow\\\\ETL\\\\Flow",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrame</span>
                </div>
                            `
            return div
        },
        apply: snippet("from(" + "$" + "{" + "1:extractor" + "}" + ")"),
        boost: 10
    },        {
        label: "process",
        type: "method",
        detail: "Flow\\\\ETL\\\\Flow",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">process</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrame</span>
                </div>
                            `
            return div
        },
        apply: snippet("process(" + "$" + "{" + "1:rows" + "}" + ")"),
        boost: 10
    },        {
        label: "read",
        type: "method",
        detail: "Flow\\\\ETL\\\\Flow",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">read</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrame</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for Flow::extract function.
                </div>
                            `
            return div
        },
        apply: snippet("read(" + "$" + "{" + "1:extractor" + "}" + ")"),
        boost: 10
    }    ]

/**
 * Flow method completion source for CodeMirror
 * @param {CompletionContext} context
 * @returns {CompletionResult|null}
 */
export function flowCompletions(context) {
    // Get text before cursor (potentially across multiple lines)
    // Look back up to 2000 characters to find the pattern
    const maxLookback = 2000
    const docText = context.state.doc.toString()
    const startPos = Math.max(0, context.pos - maxLookback)
    const textBefore = docText.slice(startPos, context.pos)

    // Check if we're directly after -> (method chaining context)
    if (!new RegExp('->\\w*$').test(textBefore)) {
        return null
    }

    // Check if we're after a Flow-returning function: df( or data_frame(
    // Find all matching functions and check if any are at top level with ->
    const flowFuncPattern = new RegExp('\\b(' + flowFunctions.join('|') + ')\\s*\\(', 'g')
    let matches = []
    let match

    while ((match = flowFuncPattern.exec(textBefore)) !== null) {
        matches.push({ name: match[1], index: match.index, endOfName: match.index + match[0].length })
    }

    // Walk backwards from cursor tracking parenthesis depth
    let depth = 0
    let i = textBefore.length - 1

    // Skip back past the -> and any word being typed
    while (i >= 0 && /[\w>-]/.test(textBefore[i])) {
        i--
    }

    // Now count parentheses going backwards
    while (i >= 0) {
        if (textBefore[i] === ')') depth++
        else if (textBefore[i] === '(') {
            depth--
            // If we're back to depth 0, check if this ( belongs to a Flow function
            if (depth === 0) {
                // Look backwards to find the function name
                let funcEnd = i
                while (funcEnd > 0 && /\s/.test(textBefore[funcEnd - 1])) {
                    funcEnd--
                }
                let funcStart = funcEnd
                while (funcStart > 0 && /\w/.test(textBefore[funcStart - 1])) {
                    funcStart--
                }
                const funcName = textBefore.slice(funcStart, funcEnd)

                // Check if this is a Flow-returning function
                if (flowFunctions.includes(funcName)) {
                    // This is it! We're directly after this function call
                    return continueWithCompletions()
                }
                // If not, we're inside some other call
                return null
            }
        }
        i--
    }

    return null

    function continueWithCompletions() {
        // Match word being typed (method name after ->)
        const word = context.matchBefore(/\w*/)

        // If no word and not explicit, don't show completions
        if (!word && !context.explicit) {
            return null
        }

        // Filter methods based on what's being typed
        const prefix = word ? word.text.toLowerCase() : ''
        const options = flowMethods.filter(method =>
            !prefix || method.label.toLowerCase().startsWith(prefix)
        )

        return {
            from: word ? word.from : context.pos,
            options: options,
            validFor: new RegExp('^\\w*$')  // Reuse while typing word characters
        }
    }
}
