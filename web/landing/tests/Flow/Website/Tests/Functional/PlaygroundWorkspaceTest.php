<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundWorkspaceTest extends EndToEndTestCase
{
    public function test_clicking_file_in_browser_shows_preview() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        $client->getCrawler()->filter('[data-playground-target="fileInput"]')->sendKeys(
            $this->createTempFile('preview-test.csv', "id,name\n1,Alice\n2,Bob")
        );
        $client->wait(2);

        $client->getCrawler()->filter('.file-tree-item.file[data-file-path*="preview-test.csv"]')->click();
        $client->wait(1);

        self::assertNotEquals('none', $client->getCrawler()->filter('[data-playground-target="filePreviewContainer"]')->getCssValue('display'));
        self::assertStringContainsString('Alice', $client->getCrawler()->filter('[data-playground-target="filePreviewContent"]')->text());
        self::assertStringContainsString('Bob', $client->getCrawler()->filter('[data-playground-target="filePreviewContent"]')->text());
    }

    public function test_file_browser_updates_after_code_creates_files() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        $this->setPlaygroundCode(
            $client,
            <<<'PHP'
<?php
file_put_contents('/workspace/generated.txt', 'Generated content');
echo 'File created';
PHP
        );

        $client->getCrawler()->filter('[data-playground-target="runButton"]')->click();
        $client->waitForElementToContain('[data-playground-target="output"]', 'File created', 5);

        self::assertStringContainsString('generated.txt', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
    }

    public function test_workspace_displays_default_structure() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        self::assertStringContainsString('bin', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
        self::assertStringContainsString('data', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
        self::assertStringContainsString('vendor', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
        self::assertStringContainsString('tools', $client->getCrawler()->filter('[data-playground-target="fileBrowserContent"]')->text());
    }
}
