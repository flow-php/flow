import { CompletionContext } from "@codemirror/autocomplete"

/**
 * Flow DSL custom completion source
 *
 * This will be populated with Flow-specific completions in future tasks.
 * For now, it returns null (no custom completions).
 *
 * Reference for implementation:
 * - Flow DSL JSON data structure (if available)
 * - CodeMirror autocomplete documentation
 */
export function flowCompletions(context) {
    return null
}
