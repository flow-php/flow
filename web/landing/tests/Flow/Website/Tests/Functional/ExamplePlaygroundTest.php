<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function mb_strtolower;

final class ExamplePlaygroundTest extends EndToEndTestCase
{
    public function test_back_to_example_link_points_at_the_example(): void
    {
        $href = $this
            ->pageOf($this->playwrightBrowser()->visit('/playground/caching/cache'))
            ->getByRole('link', ['name' => 'Back to Example'])
            ->getAttribute('href');

        static::assertNotNull($href);
        static::assertStringContainsString('/caching/cache/', $href);
        static::assertStringNotContainsString('/playground/', $href);
    }

    public function test_example_code_is_preloaded_in_playground(): void
    {
        $code = $this->getPlaygroundCode($this->pageOf($this->openPlayground('/playground/caching/cache')));

        static::assertStringContainsString('<?php', $code);
        static::assertStringContainsString('cache', $code);
    }

    public function test_example_playground_can_run_code(): void
    {
        $browser = $this->openPlayground('/playground/filesystem/stdout');

        $this->pageOf($browser)->evaluate('() => document.getElementById("action-run").click()');

        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'Files List')->assertSeeIn(
            '[data-playground-output-target="container"]',
            'Files List',
        );
    }

    public function test_example_runs_and_share_are_reported(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $browser->visit('/playground/filesystem/stdout');
        $page = $this->pageOf($browser);
        $this->waitForWasmReady($page);
        $this->acceptDialogs($page);

        $page->evaluate('() => document.getElementById("action-run").click()');
        $this->capturedEventsWithoutDurations($browser, 2);

        $page->evaluate('() => document.getElementById("action-share").click()');
        $this->capturedEventsWithoutDurations($browser, 3);

        $page->click('.cm-line >> nth=0');
        $page->keyboard()->type(' // edited');
        $page->evaluate('() => document.getElementById("action-run").click()');

        $run = static fn(bool $isCodeModified): array => [
            'name' => 'playground:code_run',
            'properties' => [
                'is_success' => true,
                'failure_reason' => null,
                'is_example' => true,
                'is_code_modified' => $isCodeModified,
            ],
        ];

        static::assertSame(
            [
                ['name' => 'playground:runtime_load', 'properties' => ['is_success' => true, 'failure_reason' => null]],
                $run(false),
                [
                    'name' => 'playground:snippet_share',
                    'properties' => ['outcome' => 'example_link', 'file_count' => 0],
                ],
                $run(true),
            ],
            $this->capturedEventsWithoutDurations($browser, 4),
        );
    }

    public function test_loaded_from_example_indicator_is_shown(): void
    {
        $this
            ->openPlayground('/playground/caching/cache')
            ->waitUntilSeeIn('[data-playground-target="storageIndicator"]', 'Loaded from example')
            ->assertSeeIn('[data-playground-target="storageIndicator"]', 'Loaded from example');
    }

    public function test_option_example_code_is_preloaded(): void
    {
        $code = $this->getPlaygroundCode($this->pageOf($this->openPlayground('/playground/reading/csv')));

        static::assertStringContainsString('<?php', $code);
        static::assertStringContainsString('csv', mb_strtolower($code));
    }
}
