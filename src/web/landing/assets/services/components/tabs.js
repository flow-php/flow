export class Tabs
{
    /**
     * @type {?HTMLAnchorElement}
     */
    #currentLink = null;

    /**
     * @type {?HTMLAnchorElement}
     */
    #currentTopic = null;

    /**
     * @type {HTMLElement}
     */
    #container;

    /**
     * @param {HTMLElement} container
     */
    constructor(container)
    {
        if (!(container instanceof HTMLElement))
        {
            throw new Error('Tabs should get html element to work on.');
        }

        this.#container = container;
    }

    /**
     * @param {string} hash
     */
    onHashChange(hash)
    {
        if (null !== this.#currentLink)
        {
            this.#currentLink.classList.remove('active');
            this.#currentLink = null;
        }

        if (null !== this.#currentTopic)
        {
            this.#currentTopic.classList.remove('active');
            this.#currentTopic = null;
        }

        const link = this.#container.querySelector(`a[href="${hash}"]`);
        const topicId = link?.getAttribute('data-topic');

        if (link)
        {
            link.classList.add('active');
            /**
             * @note It would be nice to have behavior:smooth scroll here,
             * but looks like chrome engine has a bug that cancels given scroll when another one is created.
             * Issue tracker: https://bugs.chromium.org/p/chromium/issues/detail?id=833617.
             */
            link.scrollIntoView({behavior: "instant", block: "nearest"});
            this.#currentLink = link;
        }

        if (topicId)
        {
            const topic = this.#container.querySelector(`a[href="${topicId}"]`);

            topic.classList.add('active');
            topic.scrollIntoView({behavior: "instant", block: "nearest"});
            this.#currentTopic = topic;
        }
    }
}