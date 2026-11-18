<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundRunCodeTest extends EndToEndTestCase
{
    public function test_runtime_load_share_and_runs_are_reported(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $browser->visit('/playground');
        $page = $this->pageOf($browser);
        $this->waitForWasmReady($page);

        $page->evaluate('() => document.getElementById("action-share").click()');
        $this->capturedEventsWithoutDurations($browser, 2);

        $page->evaluate('() => document.getElementById("action-run").click()');
        $this->capturedEventsWithoutDurations($browser, 3);

        foreach ([
            "<?php\necho 'unterminated;",
            "<?php\nrequire 'vendor/autoload.php';\nthrow new \\Flow\\ETL\\Foundation\\Exception\\InvalidArgumentException('flow');",
            "<?php\nthrow new \\RuntimeException('php');",
        ] as $run => $code) {
            $this->setPlaygroundCode($page, $code);
            $page->evaluate('() => document.getElementById("action-run").click()');
            $this->capturedEventsWithoutDurations($browser, 4 + $run);
        }

        $run = static fn(bool $isSuccess, ?string $failureReason): array => [
            'name' => 'playground:code_run',
            'properties' => [
                'is_success' => $isSuccess,
                'failure_reason' => $failureReason,
                'is_example' => false,
                'is_code_modified' => false,
            ],
        ];

        static::assertSame(
            [
                ['name' => 'playground:runtime_load', 'properties' => ['is_success' => true, 'failure_reason' => null]],
                ['name' => 'playground:snippet_share', 'properties' => ['outcome' => 'refused', 'file_count' => 0]],
                $run(true, null),
                $run(false, 'parse'),
                $run(false, 'flow'),
                $run(false, 'php'),
            ],
            $this->capturedEventsWithoutDurations($browser, 6),
        );
    }

    public function test_run_simple_flow_pipeline(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            require 'vendor/autoload.php';
            use function Flow\ETL\DSL\{df, from_array, to_output};
            df()->read(from_array([['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']]))->write(to_output(truncate: false))->run();
            PHP);

        $page->evaluate('() => document.getElementById("action-run").click()');

        $browser
            ->waitUntilSeeIn('[data-playground-output-target="container"]', 'Alice')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Alice')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Bob');
    }
}
