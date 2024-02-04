import {Controller} from '@hotwired/stimulus';
import hljs from 'highlight.js/lib/core';
import php from 'highlight.js/lib/languages/php';

export default class extends Controller
{
    static afterLoad(identifier, application)
    {
        hljs.registerLanguage('php', php);
    }

    connect()
    {
        hljs.highlightElement(this.element);
    }
}