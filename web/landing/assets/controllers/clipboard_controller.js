import {Controller} from '@hotwired/stimulus';
import ClipboardJS from 'clipboard';

export default class extends Controller
{
    connect()
    {
        if (!ClipboardJS.isSupported()) {
            this.element.style.display = 'none';

            return;
        }

        const clipboard = new ClipboardJS(this.element);

        clipboard.on('success', e => {
            e.clearSelection();

            this.element.classList.add('copied');
            setTimeout(() => this.element.classList.remove('copied'), 2000);
        });
    }
}