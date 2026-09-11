<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundWorkspaceTest extends EndToEndTestCase
{
    private const UPLOAD_INPUT = '[data-playground-upload-target="fileInput"]';

    public function test_clicking_file_in_browser_shows_preview(): void
    {
        $browser = $this->openPlayground('/playground');

        $this->upload($this->pageOf($browser), self::UPLOAD_INPUT, [$this->createTempFile(
            'preview-test.csv',
            "id,name\n1,Alice\n2,Bob",
        )]);

        $browser
            ->waitUntilSeeIn('[data-playground-workspace-target="tree"]', 'preview-test.csv')
            ->click('.file-tree-item.file[data-file-path*="preview-test.csv"]')
            ->waitUntilVisible('[data-playground-tabs-target="previewTab"]')
            ->assertVisible('[data-playground-tabs-target="previewTab"]')
            ->assertSeeIn('[data-playground-tabs-target="previewTabName"]', 'preview-test.csv')
            ->assertSeeIn('[data-playground-tabs-target="previewPanel"]', 'Alice')
            ->assertSeeIn('[data-playground-tabs-target="previewPanel"]', 'Bob');
    }

    public function test_file_browser_updates_after_code_creates_files(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            file_put_contents('/workspace/generated.txt', 'Generated content');
            echo 'File created';
            PHP);

        $page->evaluate('() => document.getElementById("action-run").click()');
        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'File created');

        // the tree does not watch the WASM filesystem, so it has to be refreshed by hand
        $page->evaluate(
            '() => window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-workspace").refreshTree()',
        );

        $browser->waitUntilSeeIn('[data-playground-workspace-target="tree"]', 'generated.txt')->assertSeeIn(
            '[data-playground-workspace-target="tree"]',
            'generated.txt',
        );
    }

    public function test_workspace_displays_default_structure(): void
    {
        $browser = $this->openPlayground('/playground');

        foreach (['bin', 'data', 'vendor', 'tools'] as $directory) {
            $browser->assertSeeIn('[data-playground-workspace-target="tree"]', $directory);
        }
    }
}
