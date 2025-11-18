import { Controller } from '@hotwired/stimulus';

export default class extends Controller {
    static outlets = ['code-editor'];

    #storageKey = 'flow-playground-code';
    #hasUnsavedChanges = false;
    #beforeUnloadHandler;

    connect() {
        this.#setupBeforeUnloadListener();
    }

    initialize() {
        this.onCodeChanged = this.onCodeChanged.bind(this);
        this.element.addEventListener('code-editor:code-changed', this.onCodeChanged);
    }

    onCodeChanged() {
        this.markAsChanged();
    }

    disconnect() {
        this.#removeBeforeUnloadListener();
        this.element.removeEventListener('code-editor:code-changed', this.onCodeChanged);
    }

    codeEditorOutletConnected() {
        const hasSharedCode = new URLSearchParams(window.location.search).has('c');

        if (!hasSharedCode) {
            this.#loadCodeFromStorage();
        }
    }

    saveCode() {
        if (!this.hasCodeEditorOutlet) {
            return;
        }

        const code = this.codeEditorOutlet.getCode();
        localStorage.setItem(this.#storageKey, code);
        this.#hasUnsavedChanges = false;
    }

    clearStorage() {
        localStorage.removeItem(this.#storageKey);
        this.#hasUnsavedChanges = false;
    }

    markAsChanged() {
        this.#hasUnsavedChanges = true;
    }

    #loadCodeFromStorage() {
        if (!this.hasCodeEditorOutlet) {
            return;
        }

        const savedCode = localStorage.getItem(this.#storageKey);

        if (savedCode) {
            this.#waitForEditorAndSetValue(savedCode);
            this.dispatch('loaded-from-storage', { bubbles: true });
        }
    }

    #waitForEditorAndSetValue(code, attempts = 0) {
        const maxAttempts = 50;

        if (this.codeEditorOutlet.isReady()) {
            this.codeEditorOutlet.setValue(code);
            this.#hasUnsavedChanges = false;
        } else if (attempts < maxAttempts) {
            setTimeout(() => {
                this.#waitForEditorAndSetValue(code, attempts + 1);
            }, 100);
        }
    }

    #setupBeforeUnloadListener() {
        this.#beforeUnloadHandler = (event) => {
            if (this.#hasUnsavedChanges) {
                event.preventDefault();
                event.returnValue = '';
                return '';
            }
        };

        window.addEventListener('beforeunload', this.#beforeUnloadHandler);
    }

    #removeBeforeUnloadListener() {
        if (this.#beforeUnloadHandler) {
            window.removeEventListener('beforeunload', this.#beforeUnloadHandler);
        }
    }
}
