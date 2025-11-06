import { Controller } from "@hotwired/stimulus"
import { EditorView, basicSetup } from "codemirror"
import { EditorState, StateField, StateEffect } from "@codemirror/state"
import { Decoration } from "@codemirror/view"
import { keymap } from "@codemirror/view"
import { php } from "@codemirror/lang-php"
import { autocompletion, snippetKeymap, acceptCompletion } from "@codemirror/autocomplete"
import { indentWithTab } from "@codemirror/commands"
import { flowThemeExtension } from "../codemirror/themes/theme-flow.js"
import { flowCompletions } from "../codemirror/completions/flow.js"
import { dslCompletions } from "../codemirror/completions/dsl.js"
import { dataframeCompletions } from "../codemirror/completions/dataframe.js"
import { scalarFunctionChainCompletions } from "../codemirror/completions/scalarfunctionchain.js"
import Violation from "../models/Violation.js"

export default class extends Controller {
    #editor
    #debug = false
    #textarea
    #errorEffect
    #editorReady = false

    #log(...args) {
        if (this.#debug) {
            console.log('[CodeMirrorEditor]', ...args)
        }
    }

    #initializeEditor() {
        this.#textarea = this.element
        const initialValue = this.#textarea.value

        const editorDiv = document.createElement('div')
        editorDiv.style.width = '100%'
        editorDiv.style.height = this.#textarea.style.height || '600px'

        this.#textarea.style.display = 'none'
        this.#textarea.parentNode.insertBefore(editorDiv, this.#textarea)

        this.#errorEffect = StateEffect.define()
        const errorEffect = this.#errorEffect

        const errorField = StateField.define({
            create() {
                return Decoration.none
            },
            update(errors, tr) {
                errors = errors.map(tr.changes)
                for (const effect of tr.effects) {
                    if (effect.is(errorEffect)) {
                        errors = effect.value
                    }
                }
                return errors
            },
            provide: f => EditorView.decorations.from(f)
        })

        const state = EditorState.create({
            doc: initialValue,
            extensions: [
                basicSetup,
                php(),
                flowThemeExtension,
                errorField,
                keymap.of(snippetKeymap),
                keymap.of([indentWithTab]),
                autocompletion({
                    override: [flowCompletions, dataframeCompletions, scalarFunctionChainCompletions, dslCompletions],
                    activateOnTyping: true,
                    maxRenderedOptions: 20
                }),
                EditorView.updateListener.of((update) => {
                    if (update.docChanged) {
                        this.#textarea.value = update.state.doc.toString()
                        this.dispatch('code-changed', { bubbles: true })
                    }
                })
            ]
        })

        this.#editor = new EditorView({
            state,
            parent: editorDiv
        })

        this.#editorReady = true
        this.#log('CodeMirror editor initialized')
    }

    connect() {
        this.#debug = this.application.debug
        this.#log('Connecting CodeMirror editor controller')
        this.#initializeEditor()
    }

    isReady() {
        return this.#editorReady && this.#editor !== null
    }

    disconnect() {
        if (this.#editor) {
            this.#editor.destroy()
            this.#editor = null
        }
    }

    getCode() {
        return this.#editor ? this.#editor.state.doc.toString() : ''
    }

    setValue(code) {
        this.#log('setValue called with code length:', code?.length)

        if (this.#editor) {
            this.#log('Setting value immediately')
            this.#editor.dispatch({
                changes: {
                    from: 0,
                    to: this.#editor.state.doc.length,
                    insert: code
                }
            })
        }
    }

    highlightError(errorInfo) {
        this.#log('highlightError called with:', errorInfo)

        if (!this.#editor || !errorInfo) {
            this.#log('No editor or errorInfo:', { hasEditor: !!this.#editor, errorInfo })
            return
        }

        this.clearErrors()

        const { line, type, message } = errorInfo
        if (!line) {
            this.#log('No line number in errorInfo')
            return
        }

        this.#log('Highlighting line:', line, 'with message:', message)

        const violation = Violation.create({
            message: `${type}: ${message}`,
            line: line,
            column: 0,
            startLine: line,
            endLine: line,
            startColumn: 0,
            endColumn: Number.MAX_SAFE_INTEGER
        })

        const decorations = []
        const doc = this.#editor.state.doc

        try {
            const from = violation.getStartPosition(doc)
            const to = violation.getEndPosition(doc)

            if (from < to && from >= 0 && to <= doc.length) {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-error',
                        attributes: { title: violation.message }
                    }).range(from, to)
                )
            }
        } catch (error) {
            console.warn('Failed to highlight error:', violation, error)
        }

        if (decorations.length > 0) {
            this.#editor.dispatch({
                effects: this.#errorEffect.of(Decoration.set(decorations))
            })

            const editorLine = line - 1
            const lineObj = doc.line(editorLine + 1)
            this.#editor.dispatch({
                selection: { anchor: lineObj.from },
                scrollIntoView: true
            })
        }
    }

    clearErrors() {
        if (!this.#editor) {
            return
        }

        this.#log('Clearing errors')

        this.#editor.dispatch({
            effects: this.#errorEffect.of(Decoration.none)
        })
    }
}
