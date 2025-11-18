<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundUploadTest extends EndToEndTestCase
{
    public function test_upload_multiple_files() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        $client->getCrawler()->filter('[data-playground-target="fileInput"]')->sendKeys($this->createTempFile('data.csv', "id,value\n1,100"));
        $client->wait(1);
        $client->getCrawler()->filter('[data-playground-target="fileInput"]')->sendKeys($this->createTempFile('data.json', '{"id": 1, "value": 100}'));
        $client->wait(2);

        self::assertStringContainsString('data.csv', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
        self::assertStringContainsString('data.json', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
    }

    public function test_upload_single_file() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        $client->getCrawler()->filter('[data-playground-target="fileInput"]')->sendKeys(
            $this->createTempFile('test.csv', "id,name\n1,Alice\n2,Bob")
        );
        $client->wait(2);

        self::assertStringContainsString('test.csv', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
    }
}
