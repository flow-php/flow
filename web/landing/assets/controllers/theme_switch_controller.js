import { Controller } from '@hotwired/stimulus';

export default class extends Controller {
    static targets = ['menu', 'option', 'currentIcon'];

    #mediaQuery = null;
    #onMediaChange = null;
    #onDocumentClick = null;
    #onEscape = null;

    connect() {
        this.#mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
        this.#onMediaChange = this.#handleMediaChange.bind(this);
        this.#onDocumentClick = this.#handleDocumentClick.bind(this);
        this.#onEscape = this.#handleEscape.bind(this);

        this.#mediaQuery.addEventListener('change', this.#onMediaChange);
        document.addEventListener('click', this.#onDocumentClick);
        document.addEventListener('keydown', this.#onEscape);

        this.#syncUi();
    }

    disconnect() {
        if (this.#mediaQuery && this.#onMediaChange) {
            this.#mediaQuery.removeEventListener('change', this.#onMediaChange);
        }
        if (this.#onDocumentClick) {
            document.removeEventListener('click', this.#onDocumentClick);
        }
        if (this.#onEscape) {
            document.removeEventListener('keydown', this.#onEscape);
        }
    }

    toggle(event) {
        event.preventDefault();
        event.stopPropagation();

        if (!this.hasMenuTarget) return;

        this.menuTarget.classList.toggle('hidden');
    }

    select(event) {
        const choice = event.currentTarget.dataset.themeOption;

        if (choice === 'system') {
            localStorage.removeItem('theme');
        } else {
            localStorage.setItem('theme', choice);
        }

        this.#apply();
        this.#syncUi();
        this.#close();
    }

    #handleMediaChange() {
        if (this.#currentChoice() !== 'system') return;
        this.#apply();
    }

    #handleDocumentClick(event) {
        if (!this.hasMenuTarget) return;
        if (this.menuTarget.classList.contains('hidden')) return;
        if (this.element.contains(event.target)) return;

        this.#close();
    }

    #handleEscape(event) {
        if (event.key !== 'Escape') return;
        if (!this.hasMenuTarget) return;
        if (this.menuTarget.classList.contains('hidden')) return;

        this.#close();
    }

    #close() {
        if (!this.hasMenuTarget) return;
        this.menuTarget.classList.add('hidden');
    }

    #currentChoice() {
        const stored = localStorage.getItem('theme');
        return stored === 'light' || stored === 'dark' ? stored : 'system';
    }

    #resolved() {
        const choice = this.#currentChoice();
        if (choice === 'dark') return 'dark';
        if (choice === 'light') return 'light';
        return this.#mediaQuery.matches ? 'dark' : 'light';
    }

    #apply() {
        const resolved = this.#resolved();

        if (resolved === 'dark') {
            document.documentElement.setAttribute('data-theme', 'dark');
        } else {
            document.documentElement.removeAttribute('data-theme');
        }

        document.dispatchEvent(new CustomEvent('theme:changed', {
            detail: { resolved, choice: this.#currentChoice() },
        }));
    }

    #syncUi() {
        const choice = this.#currentChoice();

        this.optionTargets.forEach((el) => {
            const isActive = el.dataset.themeOption === choice;
            el.setAttribute('aria-checked', isActive ? 'true' : 'false');
            el.dataset.active = isActive ? 'true' : 'false';
        });

        if (this.hasCurrentIconTarget) {
            this.currentIconTargets.forEach((el) => {
                const matches = el.dataset.themeIcon === choice;
                el.classList.toggle('hidden', !matches);
            });
        }
    }
}
