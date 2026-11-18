<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function array_fill;
use function str_repeat;

final class SiteSearchTest extends EndToEndTestCase
{
    private const string OPEN = '() => document.getElementById("search-modal").showPopover()';

    private const string CLOSE = '() => document.getElementById("search-modal").hidePopover()';

    private const string SUBSCRIBED = '() => Boolean(window.__pagefindHandlers.results)';

    private const string IS_OPEN = '() => document.querySelector("#search-modal:popover-open") !== null';

    public function test_failed_bundle_load_keeps_the_modal_working(): void
    {
        $browser = $this->playwrightBrowser();
        $page = $this->pageOf($browser);
        $page->addInitScript(
            'window.__failures = []; window.addEventListener("unhandledrejection", (event) => window.__failures.push(String(event.reason))); window.addEventListener("error", (event) => window.__failures.push(event.message));',
        );
        $browser->visit('/');

        $page->keyboard()->press('/');
        $page->waitForFunction(self::IS_OPEN);
        $page->evaluate(self::CLOSE);
        $page->keyboard()->press('/');
        $page->waitForFunction(self::IS_OPEN);
        $browser->wait(300);

        static::assertSame([], $page->evaluate('() => window.__failures'));
    }

    public function test_modal_sessions_report_one_results_view_each(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $this->fakePagefind($browser);
        $browser->visit('/');
        $page = $this->pageOf($browser);

        foreach ([
            [],
            [['par', 0], ['parquet', 2]],
            [['nothing to find', 0]],
            [['  ' . str_repeat('Parquet Files ', 6), 1]],
            [['ask me@example.com', 4]],
            [['abcdefghijklmnop1234', 5]],
        ] as $searches) {
            $page->evaluate(self::OPEN);
            $page->waitForFunction(self::SUBSCRIBED);

            foreach ($searches as [$term, $resultCount]) {
                $this->emitPagefind($browser, 'search', [$term, []]);
                $this->emitPagefind($browser, 'results', [['results' => array_fill(0, $resultCount, ['id' => 'x'])]]);
            }

            $page->evaluate(self::CLOSE);
        }

        $view = static fn(?string $query, int $count): array => [
            'name' => 'search:results_view',
            'properties' => ['search_query' => $query, 'result_count' => $count],
        ];

        static::assertSame(
            [
                $view('parquet', 2),
                $view('nothing to find', 0),
                $view('parquet files parquet files parquet files parquet files parquet ', 1),
                $view(null, 4),
                $view(null, 5),
            ],
            $this->capturedEventsWithoutDurations($browser, 5),
        );
    }

    public function test_result_click_sends_view_then_click(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $this->fakePagefind($browser);
        $browser->visit('/');
        $page = $this->pageOf($browser);

        $page->evaluate(self::OPEN);
        $page->waitForFunction(self::SUBSCRIBED);
        $this->emitPagefind($browser, 'search', ['Parquet', []]);
        $this->emitPagefind($browser, 'results', [['results' => [['id' => 'a'], ['id' => 'b'], ['id' => 'c']]]]);
        $page->evaluate(
            '() => document.getElementById("site-search").insertAdjacentHTML("beforeend", \'<li class="pf-result" data-pf-result-index="1"><div class="pf-result-card"><div class="pf-result-content"><p class="pf-result-title"><a class="pf-result-link" href="#result-2">Result</a></p></div></div></li>\')',
        );

        $page->click('#site-search a.pf-result-link');
        $page->evaluate(self::CLOSE);
        $browser->wait(300);

        static::assertSame(
            [
                [
                    'name' => 'search:results_view',
                    'properties' => ['search_query' => 'parquet', 'result_count' => 3],
                ],
                [
                    'name' => 'search:result_click',
                    'properties' => ['search_query' => 'parquet', 'result_position' => 2],
                ],
            ],
            $this->capturedEvents($browser),
        );
    }
}
