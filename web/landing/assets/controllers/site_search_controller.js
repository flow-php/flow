import { Controller } from '@hotwired/stimulus';

export default class extends Controller {
    static targets = ['mount'];
    static values = { bundlePath: String };

    #ui = null;
    #onKey = null;
    #onModalToggle = null;

    async connect() {
        this.#onKey = this.#handleKey.bind(this);
        this.#onModalToggle = this.#handleModalToggle.bind(this);

        document.addEventListener('keydown', this.#onKey);
        this.element.addEventListener('toggle', this.#onModalToggle);
    }

    disconnect() {
        if (this.#onKey) {
            document.removeEventListener('keydown', this.#onKey);
        }

        if (this.#onModalToggle) {
            this.element.removeEventListener('toggle', this.#onModalToggle);
        }
    }

    async #ensureUi() {
        if (this.#ui) return;
        if (!this.hasMountTarget) return;

        await import(this.bundlePathValue + 'pagefind-ui.js');

        // eslint-disable-next-line no-undef
        this.#ui = new PagefindUI({
            element: this.mountTarget,
            bundlePath: this.bundlePathValue,
            showSubResults: true,
            showImages: false,
            pageSize: 6,
            excerptLength: 24,
            resetStyles: false,
            translations: { placeholder: 'Search documentation...' },
        });
    }

    #handleKey(event) {
        if (event.key !== '/') return;

        const target = event.target;
        const tag = (target?.tagName || '').toLowerCase();

        if (tag === 'input' || tag === 'textarea' || target?.isContentEditable) return;

        event.preventDefault();
        this.element.showPopover();
    }

    async #handleModalToggle(event) {
        if (event.newState !== 'open') return;

        await this.#ensureUi();

        setTimeout(() => {
            const input = this.element.querySelector('.pagefind-ui__search-input');
            input?.focus();
        }, 50);
    }
}
