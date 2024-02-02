import { Controller } from '@hotwired/stimulus';
import {Tabs} from "../services/components/tabs.js";

export default class extends Controller
{
    connect()
    {
        const tabs = new Tabs(this.element);

        tabs.onHashChange(window.location.hash)
        window.addEventListener('hashchange', event => tabs.onHashChange(event.target.location.hash));
    }
}