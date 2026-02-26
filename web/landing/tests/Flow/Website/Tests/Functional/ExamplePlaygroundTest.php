<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class ExamplePlaygroundTest extends EndToEndTestCase
{
    public function test_back_to_example_link_navigates_correctly() : void
    {
        $client = self::navigateWithRetry('/playground/data_frame/cache');

        $this->waitForWasmReady($client);

        $client->clickLink('Back to Example');

        self::assertStringContainsString('/data_frame/cache/', $client->getCurrentURL());
        self::assertStringNotContainsString('/playground/', $client->getCurrentURL());
    }

    public function test_example_code_is_preloaded_in_playground() : void
    {
        $client = self::navigateWithRetry('/playground/data_frame/cache');

        $this->waitForWasmReady($client);

        $code = $this->getPlaygroundCode($client);

        self::assertStringContainsString('<?php', $code);
        self::assertStringContainsString('cache', $code);
    }

    public function test_example_playground_can_run_code() : void
    {
        $client = self::navigateWithRetry('/playground/filesystem/stdout');

        $this->waitForWasmReady($client);

        $client->executeScript('document.getElementById("action-run").click();');

        $client->waitForElementToContain(
            '[data-playground-output-target="container"]',
            'Files List',
            30
        );

        self::assertSelectorTextContains(
            '[data-playground-output-target="container"]',
            'Files List'
        );
    }

    public function test_loaded_from_example_indicator_is_shown() : void
    {
        $client = self::navigateWithRetry('/playground/data_frame/cache');

        $this->waitForWasmReady($client);

        $client->waitForElementToContain(
            '[data-playground-target="storageIndicator"]',
            'Loaded from example',
            10
        );

        self::assertSelectorTextContains(
            '[data-playground-target="storageIndicator"]',
            'Loaded from example'
        );
    }

    public function test_option_example_code_is_preloaded() : void
    {
        $client = self::navigateWithRetry('/playground/data_frame/data_reading/csv');

        $this->waitForWasmReady($client);

        $code = $this->getPlaygroundCode($client);

        self::assertStringContainsString('<?php', $code);
        self::assertStringContainsString('csv', \strtolower($code));
    }
}
