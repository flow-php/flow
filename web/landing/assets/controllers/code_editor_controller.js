import { Controller } from "@hotwired/stimulus"
import ace from "ace-builds"
import "../ace-themes/theme-flow.js"

export default class extends Controller {
    #editor
    #errorMarkers = []
    #initialValueSet = false
    #fullyInitialized = false
    #pendingValue = null
    #debug = false

    connect() {
        this.#debug = this.application.debug
        ace.config.set('basePath', 'https://cdn.jsdelivr.net/npm/ace-builds@1.36.5/src-noconflict/')

        const textarea = this.element;
        const editorDiv = document.createElement('div');
        editorDiv.style.width = '100%';
        editorDiv.style.height = textarea.style.height || '600px';

        textarea.style.display = 'none';
        textarea.parentNode.insertBefore(editorDiv, textarea);

        this.#editor = ace.edit(editorDiv);
        this.#editor.setTheme('ace/theme/flow');
        this.#editor.session.setMode(this.modeValue);

        const query = new URLSearchParams(window.location.search);
        if (!query.has('c')) {
            this.#editor.setValue(textarea.value, -1);
            this.#initialValueSet = true;
        }
        this.#editor.session.setUseWorker(false);
        this.#editor.setShowPrintMargin(false);
        this.#editor.setFontSize(16);

        ace.config.loadModule("ace/ext/language_tools", (langTools) => {
            this.#editor.setOptions({
                enableBasicAutocompletion: true,
                enableLiveAutocompletion: true,
                enableSnippets: true
            });

            this.#fullyInitialized = true

            if (this.#pendingValue !== null) {
                this.#log('Applying pending value:', this.#pendingValue)
                this.#editor.setValue(this.#pendingValue, -1)
                this.#pendingValue = null
            }
        });

        this.#editor.session.on('change', () => {
            textarea.value = this.#editor.getValue()
        })
    }

    disconnect() {
        if (this.#editor) {
            this.#editor.destroy();
            this.#editor = null;
        }
    }

    get modeValue() {
        return 'ace/mode/php';
    }

    // Public API for getting code (used by outlets)
    getCode() {
        return this.#editor ? this.#editor.getValue() : ''
    }

    // Public API for setting code (used by outlets)
    setValue(code) {
        this.#log('setValue called with code:', code)
        this.#log('Fully initialized:', this.#fullyInitialized)

        if (this.#fullyInitialized && this.#editor) {
            this.#log('Setting value immediately')
            this.#editor.setValue(code, -1)
        } else {
            this.#log('Queuing value for later')
            this.#pendingValue = code
        }
    }

    // Public API for highlighting error lines
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

        // Convert 1-based line number to 0-based
        const editorLine = line - 1

        // Add error annotation (displays in gutter and shows tooltip on hover)
        const annotations = this.#editor.session.getAnnotations()
        annotations.push({
            row: editorLine,
            column: 0,
            text: `${type}: ${message}`,
            type: 'error'
        })
        this.#editor.session.setAnnotations(annotations)
        this.#log('Annotations set:', annotations)

        const Range = ace.require('ace/range').Range
        const range = new Range(editorLine, 0, editorLine, 1)
        const markerId = this.#editor.session.addMarker(
            range,
            'ace_error-line',
            'fullLine',
            false
        )
        this.#errorMarkers.push(markerId)
        this.#log('Marker added with ID:', markerId)

        // Scroll to the error line
        this.#editor.scrollToLine(editorLine, true, true, () => {})
    }

    // Public API for clearing error highlights
    clearErrors() {
        if (!this.#editor) {
            return
        }

        // Remove all error markers
        this.#errorMarkers.forEach(markerId => {
            this.#editor.session.removeMarker(markerId)
        })
        this.#errorMarkers = []

        // Clear annotations
        this.#editor.session.clearAnnotations()
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[CodeEditor]', ...args)
        }
    }
}
