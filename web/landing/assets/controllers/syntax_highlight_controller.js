import {Controller} from '@hotwired/stimulus';
import 'highlight.js/styles/github-dark.min.css';
import php from 'highlight.js/lib/languages/php';
import hljs from 'highlight.js/lib/core';

/* stimulusFetch: 'lazy' */
export default class extends Controller
{
    initialize()
    {
        hljs.registerLanguage('php', php);
    }

    connect()
    {
        hljs.highlightElement(this.element);
    }
}