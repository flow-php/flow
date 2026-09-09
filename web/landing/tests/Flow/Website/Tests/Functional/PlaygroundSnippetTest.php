<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function Flow\Types\DSL\type_string;
use function getenv;
use function parse_str;
use function parse_url;

final class PlaygroundSnippetTest extends EndToEndTestCase
{
    public function test_create_and_load_snippet(): void
    {
        if (getenv('FLOW_RUN_FLAKY_TESTS') !== '1') {
            static::markTestSkipped('This test is flaky and fails randomly on GitHub Actions, need to debug it more');
        }

        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            require 'vendor/autoload.php';
            use function Flow\ETL\DSL\{df, from_array, to_output};
            df()->read(from_array([['id' => 1, 'name' => 'Snippet Test']]))->write(to_output(truncate: false))->run();
            PHP);

        $page->evaluate('() => { window.prompt = () => null; document.getElementById("action-share").click(); }');
        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'copied to clipboard');

        $currentUrl = $page->url();

        static::assertStringContainsString('/playground?snippet=', $currentUrl);

        $query = [];
        parse_str(parse_url($currentUrl, PHP_URL_QUERY) ?: '', $query);
        $snippetId = $query['snippet'] ?? null;

        static::assertNotNull($snippetId);

        $browser->visit('/playground?snippet=' . type_string()->assert($snippetId));
        $this->waitForWasmReady($page);

        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'Snippet loaded successfully');

        $loadedCode = $this->getPlaygroundCode($page);

        static::assertStringContainsString('Snippet Test', $loadedCode);
        static::assertStringContainsString('from_array', $loadedCode);
    }

    public function test_load_nonexistent_snippet_shows_error(): void
    {
        $this
            ->openPlayground('/playground?snippet=nonexistent123')
            ->waitUntilSeeIn('[data-playground-output-target="container"]', 'Failed to load snippet')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Failed to load snippet');
    }

    public function test_share_requires_non_empty_code(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, '');
        $page->evaluate('() => document.getElementById("action-share").click()');
        $browser->wait(2000);

        static::assertStringNotContainsString('snippet=', $page->url());
    }
}
