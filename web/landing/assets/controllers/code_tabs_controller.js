import { Controller } from '@hotwired/stimulus';

const STORAGE_KEY = 'code-tab';
const CHANGE_EVENT = 'code-tabs:change';

export default class extends Controller {
    static targets = ['tab', 'panel'];

    #onChange = null;

    connect() {
        this.#onChange = (event) => this.#show(event.detail.name);
        document.addEventListener(CHANGE_EVENT, this.#onChange);

        this.element.classList.add('code-tabs-ready');
        this.#show(this.#stored());
    }

    disconnect() {
        document.removeEventListener(CHANGE_EVENT, this.#onChange);
    }

    select({ params: { name } }) {
        try {
            localStorage.setItem(STORAGE_KEY, name);
        } catch (error) {
            // storage can be blocked, the choice then lasts for this page only
        }

        document.dispatchEvent(new CustomEvent(CHANGE_EVENT, { detail: { name } }));
    }

    #show(name) {
        const names = this.panelTargets.map((panel) => panel.dataset.tab);
        const selected = this.tabTargets.find((tab) => tab.getAttribute('aria-selected') === 'true');
        const active = names.includes(name) ? name : (selected?.dataset.codeTabsNameParam ?? names[0]);

        this.tabTargets.forEach((tab) => tab.setAttribute('aria-selected', String(tab.dataset.codeTabsNameParam === active)));
        this.panelTargets.forEach((panel) => { panel.hidden = panel.dataset.tab !== active; });
    }

    #stored() {
        try {
            return localStorage.getItem(STORAGE_KEY);
        } catch (error) {
            return null;
        }
    }
}
