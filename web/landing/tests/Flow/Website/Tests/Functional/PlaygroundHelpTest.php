<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundHelpTest extends EndToEndTestCase
{
    public function test_help_buttons_show_help_section_with_correct_content(): void
    {
        $browser = $this->playwrightBrowser()->visit('/playground');
        $page = $this->pageOf($browser);

        $browser
            ->waitUntilVisible('[data-help-topic="help-about"]')
            ->assertNotVisible('[data-playground-help-target="helpSection"]')
            ->click('[data-help-topic="help-about"]')
            ->waitUntilVisible('[data-playground-help-target="helpSection"]')
            ->assertVisible('[data-playground-help-target="helpSection"]')
            ->assertSeeIn('#help-about', 'Flow PHP Playground');

        $page->evaluate('() => document.querySelector(\'[data-action="click->playground-help#close"]\').click()');
        $browser->waitUntilNotVisible('[data-playground-help-target="helpSection"]');

        $page->evaluate('() => document.querySelector(\'[data-help-topic="help-navigation"]\').click()');
        $browser
            ->waitUntilVisible('[data-playground-help-target="helpSection"]')
            ->assertVisible('[data-playground-help-target="helpSection"]')
            ->assertSeeIn('#help-navigation', 'Action Buttons');

        $page->evaluate('() => document.querySelector(\'[data-action="click->playground-help#close"]\').click()');
        $browser->waitUntilNotVisible('[data-playground-help-target="helpSection"]');

        $page->evaluate('() => document.querySelector(\'[data-help-topic="help-workspace"]\').click()');
        $browser
            ->waitUntilVisible('[data-playground-help-target="helpSection"]')
            ->assertVisible('[data-playground-help-target="helpSection"]')
            ->assertSeeIn('#help-workspace', 'Workspace');
    }

    public function test_help_close_button_hides_help_section(): void
    {
        $browser = $this->playwrightBrowser()->visit('/playground');
        $page = $this->pageOf($browser);

        $browser
            ->waitUntilVisible('[data-help-topic="help-about"]')
            ->click('[data-help-topic="help-about"]')
            ->waitUntilVisible('[data-playground-help-target="helpSection"]')
            ->assertVisible('[data-playground-help-target="helpSection"]');

        $page->evaluate('() => document.querySelector(\'[data-action="click->playground-help#close"]\').click()');

        $browser
            ->waitUntilNotVisible('[data-playground-help-target="helpSection"]')
            ->assertNotVisible('[data-playground-help-target="helpSection"]');
    }
}
