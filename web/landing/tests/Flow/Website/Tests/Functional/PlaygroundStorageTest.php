<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundStorageTest extends EndToEndTestCase
{
    public function test_clearing_storage_removes_saved_code(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, "<?php\necho 'Will be cleared';");

        static::assertNotNull($page->localStorage()->getItem('flow-playground-code'));

        $this->clearLocalStorage($page);

        static::assertNull($page->localStorage()->getItem('flow-playground-code'));

        $browser->visit('/playground');
        $this->waitForWasmReady($page);

        static::assertStringNotContainsString('Will be cleared', $this->getPlaygroundCode($page));
        static::assertStringContainsString('from_csv', $this->getPlaygroundCode($page));
    }

    public function test_reset_button_clears_storage(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, "<?php\necho 'Reset Test';");
        $stored = $page->localStorage()->getItem('flow-playground-code');

        static::assertNotNull($stored);
        static::assertStringContainsString('Reset Test', $stored);

        $this->clearLocalStorage($page);

        $browser->visit('/playground');
        $this->waitForWasmReady($page);

        static::assertStringNotContainsString('Reset Test', $this->getPlaygroundCode($page));
    }
}
