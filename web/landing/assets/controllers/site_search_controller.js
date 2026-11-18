import { Controller } from '@hotwired/stimulus';

const queryOf = (term) => {
    const trimmed = term.trim();

    if (/\S+@\S+\.\S+/.test(trimmed) || /\S{16,}/.test(trimmed)) {
        return null;
    }

    return trimmed.toLowerCase().slice(0, 64);
};

export default class extends Controller {
    static targets = ['mount'];
    static values = { bundlePath: String };

    #instance = null;
    #term = '';
    #count = 0;
    #reported = false;
    #onKey = null;
    #onModalToggle = null;
    #onResultClick = null;

    async connect() {
        this.#onKey = this.#handleKey.bind(this);
        this.#onModalToggle = this.#handleModalToggle.bind(this);
        this.#onResultClick = this.#handleResultClick.bind(this);

        document.addEventListener('keydown', this.#onKey);
        this.element.addEventListener('toggle', this.#onModalToggle);
        this.mountTarget.addEventListener('click', this.#onResultClick);
    }

    disconnect() {
        if (this.#onKey) {
            document.removeEventListener('keydown', this.#onKey);
        }

        if (this.#onModalToggle) {
            this.element.removeEventListener('toggle', this.#onModalToggle);
        }

        if (this.#onResultClick) {
            this.mountTarget.removeEventListener('click', this.#onResultClick);
        }
    }

    async #ensureUi() {
        if (this.#instance) return;

        try {
            await import(this.bundlePathValue + 'pagefind-component-ui.js');
        } catch (error) {
            console.error('[site-search] Pagefind failed to load', error);
        }

        if (!window.PagefindComponents) return;

        this.#instance = window.PagefindComponents.getInstanceManager().getInstance('default');
        this.#instance.on('search', (term) => { this.#term = term; }, this);
        this.#instance.on('results', (result) => { this.#count = result.results.length; }, this);
    }

    #reportResultsView() {
        if (this.#reported || !this.#term) return;

        this.#reported = true;
        window.posthog?.capture('search:results_view', {
            search_query: queryOf(this.#term),
            result_count: this.#count,
        });
    }

    #handleResultClick(event) {
        const result = event.target.closest('[data-pf-result-index]');

        if (!result || !event.target.closest('a')) return;

        this.#reportResultsView();
        window.posthog?.capture('search:result_click', {
            search_query: queryOf(this.#term),
            result_position: Number(result.dataset.pfResultIndex) + 1,
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
        if (event.newState !== 'open') {
            this.#reportResultsView();

            return;
        }

        this.#reported = false;

        await this.#ensureUi();

        setTimeout(() => this.element.querySelector('pagefind-input')?.focus(), 50);
    }
}
