<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundHelpTest extends EndToEndTestCase
{
    public function test_help_buttons_show_help_section_with_correct_content() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        self::assertEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));

        $client->getCrawler()->filter('[data-help-topic="help-about"]')->click();
        $client->wait(1);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));
        self::assertStringContainsString('Flow PHP Playground', $client->getCrawler()->filter('#help-about')->text());

        $client->getCrawler()->filter('[data-action="click->playground-help#close"]')->click();
        $client->wait(1);

        $client->executeScript('document.querySelector(\'[data-help-topic="help-navigation"]\').click();');
        $client->wait(1);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));
        self::assertStringContainsString('Action Buttons', $client->getCrawler()->filter('#help-navigation')->text());

        $client->getCrawler()->filter('[data-action="click->playground-help#close"]')->click();
        $client->wait(1);

        $client->executeScript('document.querySelector(\'[data-help-topic="help-workspace"]\').click();');
        $client->wait(1);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));
        self::assertStringContainsString('Workspace', $client->getCrawler()->filter('#help-workspace')->text());
    }

    public function test_help_close_button_hides_help_section() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $client->getCrawler()->filter('[data-help-topic="help-about"]')->click();
        $client->wait(1);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));

        $client->getCrawler()->filter('[data-action="click->playground-help#close"]')->click();
        $client->wait(1);

        self::assertEquals('none', $client->getCrawler()->filter('[data-playground-help-target="helpSection"]')->getCssValue('display'));
    }
}
