<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundUploadTest extends EndToEndTestCase
{
    private const INPUT = '[data-playground-upload-target="fileInput"]';

    public function test_upload_multiple_files(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->upload($page, self::INPUT, [$this->createTempFile('data.csv', "id,value\n1,100")]);
        $browser->waitUntilSeeIn('[data-playground-workspace-target="tree"]', 'data.csv');

        $this->upload($page, self::INPUT, [$this->createTempFile('data.json', '{"id": 1, "value": 100}')]);
        $browser
            ->waitUntilSeeIn('[data-playground-workspace-target="tree"]', 'data.json')
            ->assertSeeIn('[data-playground-workspace-target="tree"]', 'data.csv')
            ->assertSeeIn('[data-playground-workspace-target="tree"]', 'data.json');
    }

    public function test_upload_single_file(): void
    {
        $browser = $this->openPlayground('/playground');

        $this->upload($this->pageOf($browser), self::INPUT, [$this->createTempFile(
            'test.csv',
            "id,name\n1,Alice\n2,Bob",
        )]);

        $browser->waitUntilSeeIn('[data-playground-workspace-target="tree"]', 'test.csv')->assertSeeIn(
            '[data-playground-workspace-target="tree"]',
            'test.csv',
        );
    }
}
