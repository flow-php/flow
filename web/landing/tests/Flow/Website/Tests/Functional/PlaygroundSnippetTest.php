<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundSnippetTest extends EndToEndTestCase
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

    public function test_create_and_load_snippet() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        // Wait for storage controller to finish loading (or skip if no stored code)
        $client->wait(1);

        $testCode = <<<'PHP'
<?php
require 'vendor/autoload.php';
use function Flow\ETL\DSL\{df, from_array, to_output};
df()->read(from_array([['id' => 1, 'name' => 'Snippet Test']]))->write(to_output(truncate: false))->run();
PHP;

        $this->setPlaygroundCode($client, $testCode);

        // Click Share button
        $client->executeScript('Array.from(document.querySelectorAll(\'button\')).find(b => b.textContent.includes(\'Share\')).click();');

        // Wait for share success message
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Share link', 15);

        // Extract snippet URL from browser URL bar (share controller updates window.history)
        $currentUrl = $client->getCurrentURL();

        self::assertStringContainsString('/playground?snippet=', $currentUrl, 'URL should contain snippet parameter');

        // Extract snippet ID from current URL
        $parsedUrl = \parse_url($currentUrl);
        \parse_str($parsedUrl['query'] ?? '', $queryParams);
        $snippetId = $queryParams['snippet'] ?? null;

        self::assertNotNull($snippetId, 'Snippet ID should be extracted from URL');

        // Navigate to snippet URL to test loading
        $client->request('GET', '/playground?snippet=' . $snippetId);

        $this->waitForWasmReady($client);

        // Wait for snippet to load
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Snippet loaded successfully', 10);

        // Verify code is loaded
        $loadedCode = $this->getPlaygroundCode($client);
        self::assertStringContainsString('Snippet Test', $loadedCode);
        self::assertStringContainsString('from_array', $loadedCode);
    }

    public function test_load_nonexistent_snippet_shows_error() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground?snippet=nonexistent123');

        $this->waitForWasmReady($client);

        // Should show error message from share controller
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Failed to load snippet', 10);

        $output = $client->getCrawler()->filter('[data-playground-output-target="container"]')->text();
        self::assertStringContainsString('Failed to load snippet', $output);
    }

    public function test_share_requires_non_empty_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        // Clear the editor to have empty code
        $this->setPlaygroundCode($client, '');

        // Click Share button
        $client->executeScript('Array.from(document.querySelectorAll(\'button\')).find(b => b.textContent.includes(\'Share\')).click();');

        // Should show error or do nothing with empty code
        $client->wait(2);

        // Verify URL hasn't changed (no snippet created)
        $currentUrl = $client->getCurrentURL();
        self::assertStringNotContainsString('snippet=', $currentUrl, 'Empty code should not create snippet');
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
