import {Controller} from '@hotwired/stimulus';
import Prism from 'prismjs';
import '../prismjs/themes/prism-flow.css';
import '../prismjs/plugins/autolinker/autolinker.js';
import 'prismjs/plugins/autoloader/prism-autoloader.js';
import 'prismjs/components/prism-markup-templating.min.js';
import 'prismjs/components/prism-php.min.js';
import 'prismjs/components/prism-bash.min.js';

/* stimulusFetch: 'lazy' */
export default class extends Controller
{
    initialize()
    {
        Prism.highlightElement(this.element);
    }
}