<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundSnippetTest extends EndToEndTestCase
{
    public function test_load_nonexistent_snippet_shows_error(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $this->pageOf($browser)->addInitScript(
            'const realFetch = window.fetch; window.fetch = (url, options) => String(url).endsWith("/snippets/nonexistent123/snippet.json") ? Promise.resolve(new Response("", { status: 404 })) : realFetch(url, options);',
        );

        $browser
            ->visit('/playground?snippet=nonexistent123')
            ->waitUntilSeeIn('[data-playground-output-target="container"]', 'Failed to load snippet')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Failed to load snippet');

        static::assertContains(
            [
                'name' => 'playground:snippet_open',
                'properties' => ['is_success' => false, 'failure_reason' => 'not_found'],
            ],
            $this->capturedEvents($browser),
        );
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
