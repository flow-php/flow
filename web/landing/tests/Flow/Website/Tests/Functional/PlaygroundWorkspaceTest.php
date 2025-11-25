<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundWorkspaceTest extends EndToEndTestCase
{
    public function test_clicking_file_in_browser_shows_preview() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $client->getCrawler()->filter('[data-playground-upload-target="fileInput"]')->sendKeys(
            $this->createTempFile('preview-test.csv', "id,name\n1,Alice\n2,Bob")
        );
        $client->waitForElementToContain('[data-playground-workspace-target="tree"]', 'preview-test.csv', 5);

        $client->getCrawler()->filter('.file-tree-item.file[data-file-path*="preview-test.csv"]')->click();
        $client->waitForVisibility('[data-playground-target="filePreviewContainer"]', 3);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-target="filePreviewContainer"]')->getCssValue('display'));
        self::assertStringContainsString('Alice', $client->getCrawler()->filter('[data-playground-target="filePreviewContent"]')->text());
        self::assertStringContainsString('Bob', $client->getCrawler()->filter('[data-playground-target="filePreviewContent"]')->text());
    }

    public function test_file_browser_updates_after_code_creates_files() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode(
            $client,
            <<<'PHP'
<?php
file_put_contents('/workspace/generated.txt', 'Generated content');
echo 'File created';
PHP
        );

        $client->executeScript('document.getElementById("action-run").click();');
        $client->waitForElementToContain('[data-playground-output-target="container"]', 'File created', 5);

        // Manually refresh workspace tree after PHP code creates files
        $client->executeScript('window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-workspace").refreshTree();');
        $client->wait(1);

        self::assertStringContainsString('generated.txt', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
    }

    public function test_workspace_displays_default_structure() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');

        $this->waitForWasmReady($client);

        self::assertStringContainsString('bin', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
        self::assertStringContainsString('data', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
        self::assertStringContainsString('vendor', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
        self::assertStringContainsString('tools', $client->getCrawler()->filter('[data-playground-workspace-target="tree"]')->text());
    }
}
