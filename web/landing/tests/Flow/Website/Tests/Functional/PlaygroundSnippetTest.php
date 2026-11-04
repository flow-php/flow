<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundSnippetTest extends EndToEndTestCase
{
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
