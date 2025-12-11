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

    #[\Override]
    protected function tearDown() : void
    {
        $this->clearStorageBeforeTest();
        parent::tearDown();
    }

    public function test_clearing_storage_removes_saved_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\necho 'Will be cleared';");

        self::assertNotNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $this->clearLocalStorage($client);

        self::assertNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        self::assertStringNotContainsString('Will be cleared', $this->getPlaygroundCode($client));
        self::assertStringContainsString('from_csv', $this->getPlaygroundCode($client));
    }

    public function test_reset_button_clears_storage() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\necho 'Reset Test';");

        self::assertStringContainsString('Reset Test', $this->getFromLocalStorage($client, 'flow-playground-code'));

        $this->clearLocalStorage($client);

        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

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
