import {Tabs} from "../../../services/components/tabs.js";

describe("tabs test", () =>
{
    /**
     * @type {Spy<Func>}
     */
    let scrollIntoViewSpy = null;

    beforeEach(() =>
    {
        document.body.insertAdjacentHTML('afterbegin', `
            <div data-test-container>
                <a href="#topic_1"></a>
                <a href="#topic_2"></a>
                <a href="#topic_3"></a>
                <a href="#topic_without_examples"></a>

                <section id="topic_1">
                    <a href="#example_1" data-topic="#topic_1"></a>
                    <a href="#example_2" data-topic="#topic_1"></a>

                    <section id="example_1"></section>
                    <section id="example_2"></section>
                </section>

                <section id="topic_2">
                    <a href="#example_3" data-topic="#topic_2"></a>
                    <a href="#example_4" data-topic="#topic_2"></a>

                    <section id="example_3"></section>
                    <section id="example_4"></section>
                </section>

                <section id="topic_3">
                    <a href="#example_5" data-topic="#topic_3"></a>
                    <a href="#example_6" data-topic="#topic_3"></a>

                    <section id="example_5"></section>
                    <section id="example_6"></section>
                </section>

                <section id="topic_without_examples"></section>
            </div>
        `);

        window.Element.prototype.scrollIntoView = scrollIntoViewSpy = jasmine.createSpy('scrollIntoView');
    });

    it("should throw an error.", () =>
    {
        expect(() => new Tabs(null)).toThrowError("Tabs should get html element to work on.")
    });

    it("should add active class only to a specific topic link.", () =>
    {
        const tabs = new Tabs(document.querySelector('[data-test-container]'));

        tabs.onHashChange('#topic_2');

        expect(document.querySelector('[href="#topic_2"]')).toHaveClass('active');
        expect(scrollIntoViewSpy).toHaveBeenCalledTimes(1);
    });

    it("should add active class to a specific example & topic link.", () =>
    {
        const tabs = new Tabs(document.querySelector('[data-test-container]'));

        tabs.onHashChange('#example_5');

        expect(document.querySelector('[href="#example_5"]')).toHaveClass('active');
        expect(document.querySelector('[href="#topic_3"]')).toHaveClass('active');
        expect(scrollIntoViewSpy).toHaveBeenCalledTimes(2);
    });

    it("should remove active class after changing to non existent hash id.", () =>
    {
        const tabs = new Tabs(document.querySelector('[data-test-container]'));

        tabs.onHashChange('#example_1');

        expect(document.querySelector('[href="#example_1"]')).toHaveClass('active');
        expect(document.querySelector('[href="#topic_1"]')).toHaveClass('active');

        tabs.onHashChange('#non_existing_id');

        expect(document.querySelector('[href="#example_1"]')).not.toHaveClass('active');
        expect(document.querySelector('[href="#topic_1"]')).not.toHaveClass('active');
    });

    it("should remove active class and add it to a new example & topic after hash change.", () =>
    {
        const tabs = new Tabs(document.querySelector('[data-test-container]'));

        tabs.onHashChange('#example_2');

        expect(document.querySelector('[href="#example_2"]')).toHaveClass('active');
        expect(document.querySelector('[href="#topic_1"]')).toHaveClass('active');
        expect(document.querySelector('[href="#example_3"]')).not.toHaveClass('active');
        expect(document.querySelector('[href="#topic_2"]')).not.toHaveClass('active');

        tabs.onHashChange('#example_3');

        expect(document.querySelector('[href="#example_2"]')).not.toHaveClass('active');
        expect(document.querySelector('[href="#topic_1"]')).not.toHaveClass('active');
        expect(document.querySelector('[href="#example_3"]')).toHaveClass('active');
        expect(document.querySelector('[href="#topic_2"]')).toHaveClass('active');
    });
});