<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundUploadTest extends EndToEndTestCase
{
    public function test_upload_multiple_files() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $client->getCrawler()->filter('[data-playground-upload-target="fileInput"]')->sendKeys($this->createTempFile('data.csv', "id,value\n1,100"));
        $client->waitForElementToContain('[data-playground-workspace-target="tree"]', 'data.csv', 5);

        $client->getCrawler()->filter('[data-playground-upload-target="fileInput"]')->sendKeys($this->createTempFile('data.json', '{"id": 1, "value": 100}'));
        $client->waitForElementToContain('[data-playground-workspace-target="tree"]', 'data.json', 5);

        self::assertStringContainsString('data.csv', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
        self::assertStringContainsString('data.json', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
    }

    public function test_upload_single_file() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $client->getCrawler()->filter('[data-playground-upload-target="fileInput"]')->sendKeys(
            $this->createTempFile('test.csv', "id,name\n1,Alice\n2,Bob")
        );
        $client->waitForElementToContain('[data-playground-workspace-target="tree"]', 'test.csv', 5);

        self::assertStringContainsString('test.csv', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
    }
}
