<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class CodeTabsTest extends EndToEndTestCase
{
    private const string VISIBLE_PANELS = '() => [...document.querySelectorAll(".code-tabs")].map((group) => [...group.querySelectorAll(".code-tabs-panel")].filter((panel) => !panel.hidden).map((panel) => panel.dataset.tab).join(","))';

    public function test_a_selected_tab_opens_in_every_group_that_has_it(): void
    {
        $browser = $this->playwrightBrowser();
        $browser->visit('/guides/import-csv-file-into-mysql-or-postgresql-in-php');
        $page = $this->pageOf($browser);
        $page->waitForSelector('.code-tabs.code-tabs-ready');

        static::assertSame(
            ['MySQL', 'MySQL', 'MySQL', 'Doctrine MySQL', 'Doctrine MySQL'],
            $page->evaluate(self::VISIBLE_PANELS),
        );

        $page->click('button[role="tab"][data-code-tabs-name-param="Flow PostgreSQL"] >> nth=0');

        static::assertSame(
            ['Flow PostgreSQL', 'MySQL', 'MySQL', 'Flow PostgreSQL', 'Flow PostgreSQL'],
            $page->evaluate(self::VISIBLE_PANELS),
        );
        static::assertSame(
            ['Flow PostgreSQL', 'Flow PostgreSQL', 'Flow PostgreSQL'],
            $page->evaluate(
                '() => [...document.querySelectorAll(\'button[role="tab"][aria-selected="true"]\')].map((tab) => tab.textContent).filter((name) => name.startsWith("Flow"))',
            ),
        );
        static::assertSame('Flow PostgreSQL', $page->evaluate('() => localStorage.getItem("code-tab")'));
    }
}
