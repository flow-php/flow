<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Exception;
use Override;

final class PlaygroundStorageTest extends EndToEndTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->clearStorageBeforeTest();
    }

    #[Override]
    protected function tearDown(): void
    {
        $this->clearStorageBeforeTest();
        parent::tearDown();
    }

    public function test_clearing_storage_removes_saved_code(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\necho 'Will be cleared';");

        static::assertNotNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $this->clearLocalStorage($client);

        static::assertNull($this->getFromLocalStorage($client, 'flow-playground-code'));

        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        static::assertStringNotContainsString('Will be cleared', $this->getPlaygroundCode($client));
        static::assertStringContainsString('from_csv', $this->getPlaygroundCode($client));
    }

    public function test_reset_button_clears_storage(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, "<?php\necho 'Reset Test';");

        $storedCode = $this->getFromLocalStorage($client, 'flow-playground-code');

        static::assertNotNull($storedCode);
        static::assertStringContainsString('Reset Test', $storedCode);

        $this->clearLocalStorage($client);

        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        static::assertStringNotContainsString('Reset Test', $this->getPlaygroundCode($client));
    }

    private function clearStorageBeforeTest(): void
    {
        try {
            $client = self::navigateWithRetry('/playground');
            $client->executeScript('localStorage.clear();');
        } catch (Exception) {
        }
    }
}
