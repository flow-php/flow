import {JSDOM} from 'jsdom';

beforeEach(() =>
{
    const jsdom = new JSDOM(`<!DOCTYPE html><html lang="en"><body></body></html>`);

    global.window = jsdom.window;
    global.document = window.document;
    global.HTMLElement = window.HTMLElement;
    global.HTMLDivElement = window.HTMLDivElement;
});