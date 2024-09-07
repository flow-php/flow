import {Controller} from '@hotwired/stimulus';

/* stimulusFetch: 'lazy' */
export default class extends Controller
{
    connect()
    {
        this.element.querySelectorAll('a').forEach(link => {
            link.target = '_blank';
            link.rel = 'noopener';
        });
    }
}