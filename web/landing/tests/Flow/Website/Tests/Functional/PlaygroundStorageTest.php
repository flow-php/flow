<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundStorageTest extends EndToEndTestCase
{
    protected function setUp() : void
    {
        parent::setUp();
        $this->clearStorageBeforeTest();
    }

    protected function tearDown() : void
    {
        $this->clearStorageBeforeTest();
        parent::tearDown();
    }

    public function test_clearing_storage_removes_saved_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\necho 'Will be cleared';");
        $this->saveToLocalStorage($client);
        $client->wait(1);

        self::assertNotNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $this->clearLocalStorage($client);
        $client->wait(1);

        self::assertNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);
        $client->wait(1);

        self::assertStringNotContainsString('Will be cleared', $this->getPlaygroundCode($client));
        self::assertStringContainsString('from_csv', $this->getPlaygroundCode($client));
    }

    public function test_code_persists_in_local_storage() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\necho 'Storage Test';");
        $this->saveToLocalStorage($client);
        $client->wait(1);

        self::assertStringContainsString('Storage Test', $this->getFromLocalStorage($client, 'flow-playground-code'));

        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);
        $client->wait(1);

        self::assertStringContainsString('Storage Test', $this->getPlaygroundCode($client));
    }

    public function test_reset_button_clears_storage() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\necho 'Reset Test';");
        $this->saveToLocalStorage($client);
        $client->wait(1);

        self::assertStringContainsString('Reset Test', $this->getFromLocalStorage($client, 'flow-playground-code'));

        $this->clearLocalStorage($client);

        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);
        $client->wait(1);

        self::assertStringNotContainsString('Reset Test', $this->getPlaygroundCode($client));
    }

    private function clearStorageBeforeTest() : void
    {
        try {
            $client = self::createE2EClient();
            $client->request('GET', '/playground');
            $client->executeScript('localStorage.clear();');
        } catch (\Exception) {
        }
    }
}
