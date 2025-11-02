import { Controller } from "@hotwired/stimulus"
import ace from "ace-builds"
import "../ace-themes/theme-flow.js"

export default class extends Controller {
    #editor

    connect() {
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
        this.#editor.setValue(textarea.value, -1);
        this.#editor.session.setUseWorker(false);
        this.#editor.setShowPrintMargin(false);
        this.#editor.setFontSize(16);

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
}
