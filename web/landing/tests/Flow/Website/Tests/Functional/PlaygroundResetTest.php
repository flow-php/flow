<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundResetTest extends EndToEndTestCase
{
    public function test_reset_clears_custom_code_and_restores_default() : void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $customCode = <<<'PHP'
<?php
// This is custom test code that should be reset
echo "Custom code for reset test - " . uniqid();
PHP;

        $this->setPlaygroundCode($client, $customCode);

        $codeBeforeReset = $this->getPlaygroundCode($client);
        self::assertStringContainsString('Custom code for reset test', $codeBeforeReset);

        $client->executeScript('document.getElementById("action-reset").click();');

        $client->switchTo()->alert()->accept();

        $this->waitForWasmReady($client);

        $codeAfterReset = $this->getPlaygroundCode($client);

        self::assertStringNotContainsString('Custom code for reset test', $codeAfterReset);
        self::assertNotEquals($customCode, $codeAfterReset);
    }
}
