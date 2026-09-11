<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundResetTest extends EndToEndTestCase
{
    public function test_reset_clears_custom_code_and_restores_default(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $customCode = <<<'PHP'
            <?php
            // This is custom test code that should be reset
            echo "Custom code for reset test - " . uniqid();
            PHP;

        $this->setPlaygroundCode($page, $customCode);

        static::assertStringContainsString('Custom code for reset test', $this->getPlaygroundCode($page));

        $this->acceptDialogs($page);
        $page->evaluate('() => document.getElementById("action-reset").click()');

        $this->waitForWasmReady($page);
        $codeAfterReset = $this->getPlaygroundCode($page);

        static::assertStringNotContainsString('Custom code for reset test', $codeAfterReset);
        static::assertNotEquals($customCode, $codeAfterReset);
    }
}
