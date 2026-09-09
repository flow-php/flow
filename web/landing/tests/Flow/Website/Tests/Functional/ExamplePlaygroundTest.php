<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function mb_strtolower;

final class ExamplePlaygroundTest extends EndToEndTestCase
{
    public function test_back_to_example_link_navigates_correctly(): void
    {
        // the link is server-rendered, so this asserts navigation and must not wait on the WASM boot
        $browser = $this->playwrightBrowser()->visit('/playground/caching/cache');

        $browser->click('Back to Example');

        $url = $this->pageOf($browser)->url();

        static::assertStringContainsString('/caching/cache/', $url);
        static::assertStringNotContainsString('/playground/', $url);
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
