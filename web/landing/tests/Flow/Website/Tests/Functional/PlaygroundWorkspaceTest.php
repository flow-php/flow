<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundWorkspaceTest extends EndToEndTestCase
{
    public function test_clicking_file_in_browser_shows_preview(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $client
            ->getCrawler()
            ->filter('[data-playground-upload-target="fileInput"]')
            ->sendKeys($this->createTempFile('preview-test.csv', "id,name\n1,Alice\n2,Bob"));
        $client->waitForElementToContain('[data-playground-workspace-target="tree"]', 'preview-test.csv', 5);

        $client->getCrawler()->filter('.file-tree-item.file[data-file-path*="preview-test.csv"]')->click();
        $client->waitForVisibility('[data-playground-tabs-target="previewTab"]', 3);

        static::assertNotEquals(
            'none',
            $client->getCrawler()->filter('[data-playground-tabs-target="previewTab"]')->getCssValue('display'),
        );
        static::assertStringContainsString(
            'preview-test.csv',
            $client->getCrawler()->filter('[data-playground-tabs-target="previewTabName"]')->text(),
        );
        static::assertStringContainsString(
            'Alice',
            $client->getCrawler()->filter('[data-playground-tabs-target="previewPanel"]')->text(),
        );
        static::assertStringContainsString(
            'Bob',
            $client->getCrawler()->filter('[data-playground-tabs-target="previewPanel"]')->text(),
        );
    }

    public function test_file_browser_updates_after_code_creates_files(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, <<<'PHP'
            <?php
            file_put_contents('/workspace/generated.txt', 'Generated content');
            echo 'File created';
            PHP);

        $client->executeScript('document.getElementById("action-run").click();');
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'File created', 5);

        // Manually refresh workspace tree after PHP code creates files
        $client->executeScript(
            'window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-workspace").refreshTree();',
        );
        $client->wait(1);

        static::assertStringContainsString(
            'generated.txt',
            $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text(),
        );
    }

    public function test_workspace_displays_default_structure(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        static::assertStringContainsString(
            'bin',
            $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text(),
        );
        static::assertStringContainsString(
            'data',
            $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text(),
        );
        static::assertStringContainsString(
            'vendor',
            $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text(),
        );
        static::assertStringContainsString(
            'tools',
            $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text(),
        );
    }
}
