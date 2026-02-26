<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundSnippetTest extends EndToEndTestCase
{
    public function test_create_and_load_snippet() : void
    {
        self::markTestSkipped('This test is flaky and fails randomly on GitHub Actions, need to debug it more');

        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $client->wait(1);

        $testCode = <<<'PHP'
<?php
require 'vendor/autoload.php';
use function Flow\ETL\DSL\{df, from_array, to_output};
df()->read(from_array([['id' => 1, 'name' => 'Snippet Test']]))->write(to_output(truncate: false))->run();
PHP;

        $this->setPlaygroundCode($client, $testCode);

        $client->executeScript('document.getElementById("action-share").click();');

        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Share link', 15);

        $currentUrl = $client->getCurrentURL();

        self::assertStringContainsString('/playground?snippet=', $currentUrl, 'URL should contain snippet parameter');

        $parsedUrl = \parse_url($currentUrl);
        \parse_str($parsedUrl['query'] ?? '', $queryParams);
        $snippetId = $queryParams['snippet'] ?? null;

        self::assertNotNull($snippetId, 'Snippet ID should be extracted from URL');

        $client->request('GET', '/playground?snippet=' . $snippetId);

        $this->waitForWasmReady($client);

        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Snippet loaded successfully', 10);

        $loadedCode = $this->getPlaygroundCode($client);
        self::assertStringContainsString('Snippet Test', $loadedCode);
        self::assertStringContainsString('from_array', $loadedCode);
    }

    public function test_load_nonexistent_snippet_shows_error() : void
    {
        $client = self::navigateWithRetry('/playground?snippet=nonexistent123');

        $this->waitForWasmReady($client);

        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Failed to load snippet', 10);

        $output = $client->getCrawler()->filter('[data-playground-output-target="container"]')->text();
        self::assertStringContainsString('Failed to load snippet', $output);
    }

    public function test_share_requires_non_empty_code() : void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, '');

        $client->executeScript('document.getElementById("action-share").click();');

        $client->wait(2);

        $currentUrl = $client->getCurrentURL();
        self::assertStringNotContainsString('snippet=', $currentUrl, 'Empty code should not create snippet');
    }
}
