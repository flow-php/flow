<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundFormatCodeTest extends EndToEndTestCase
{
    public function test_format_unformatted_code(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\ndf()->read(from_array([['id'=>1,'name'=>'Test']]))->run();");

        $client->executeScript('document.getElementById("action-format").click();');
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'formatted', 10);

        static::assertStringContainsString("'id' => 1", $this->getPlaygroundCode($client));
        static::assertStringNotContainsString("'id'=>1", $this->getPlaygroundCode($client));
    }
}
