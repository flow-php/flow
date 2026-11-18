<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

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

    public function test_names_with_markup_are_shown_as_text(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            file_put_contents('/workspace/output/<i id="pwe-file">x.txt', 'probe');
            mkdir('/workspace/output/<b id="pwe-dir">d');
            file_put_contents('/workspace/output/<b id="pwe-dir">d/inner.txt', 'probe');
            file_put_contents('/workspace/output/a" data-pwe-probe="1.txt', 'probe');
            echo 'Probe written';
            PHP);

        $page->evaluate('() => document.getElementById("action-run").click()');
        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'Probe written');
        $page->evaluate(
            '() => window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-workspace").refreshTree()',
        );

        $tree = type_string()->assert($page->evaluate(
            '() => document.querySelector(\'[data-playground-workspace-target="tree"]\').textContent',
        ));

        static::assertTrue($page->evaluate('() => document.getElementById("pwe-file") === null'), 'file name');
        static::assertStringContainsString('<i id="pwe-file">x.txt', $tree);
        static::assertTrue($page->evaluate('() => document.getElementById("pwe-dir") === null'), 'directory name');
        static::assertStringContainsString('<b id="pwe-dir">d', $tree);
        static::assertTrue($page->evaluate('() => document.querySelector("[data-pwe-probe]") === null'), 'file path');
        static::assertContains(
            '/output/a" data-pwe-probe="1.txt',
            type_list(type_string())
                ->assert($page->evaluate(
                    '() => [...document.querySelectorAll(".file-tree-item.file")].map(item => item.dataset.filePath)',
                )),
        );

        $page->evaluate(
            '(path) => [...document.querySelectorAll(".file-tree-item.file")].find(item => item.dataset.filePath === path).click()',
            '/output/a" data-pwe-probe="1.txt',
        );

        $browser->waitUntilSeeIn('[data-playground-tabs-target="previewTabName"]', 'a" data-pwe-probe="1.txt');
    }

    public function test_workspace_displays_default_structure(): void
    {
        $browser = $this->openPlayground('/playground');

        foreach (['data', 'vendor', 'tools'] as $directory) {
            $browser->assertSeeIn('[data-playground-workspace-target="tree"]', $directory);
        }
    }
}
