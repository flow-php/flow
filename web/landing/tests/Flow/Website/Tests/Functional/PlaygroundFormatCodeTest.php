<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundFormatCodeTest extends EndToEndTestCase
{
    public function test_format_unformatted_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\ndf()->read(from_array([['id'=>1,'name'=>'Test']]))->run();");

        $client->executeScript('Array.from(document.querySelectorAll(\'button\')).find(b => b.textContent.includes(\'Format\')).click();');
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'formatted', 10);

        self::assertStringContainsString("'id' => 1", $this->getPlaygroundCode($client));
        self::assertStringNotContainsString("'id'=>1", $this->getPlaygroundCode($client));
    }
}
