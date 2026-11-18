<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use PHPUnit\Framework\Attributes\TestWith;

final class CodeCopyTest extends EndToEndTestCase
{
    #[TestWith(['/', 'button[data-clipboard-target="#command"]', 'install_command'])]
    #[TestWith(['/reading/array/', 'button[data-clipboard-target="#code-php"]', 'example'])]
    #[TestWith([
        '/documentation/dsl/core/ref',
        'button[data-clipboard-target^="#example-code-"] >> nth=0',
        'dsl_function',
    ])]
    #[TestWith([
        '/documentation/quick-start',
        'article button[data-clipboard-kind-value="documentation"] >> nth=0',
        'documentation',
    ])]
    public function test_copy_button_reports_the_kind_of_code(string $path, string $button, string $kind): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $browser->visit($path);
        $page = $this->pageOf($browser);

        $page->click($button);
        $page->waitForSelector('button.copied');

        static::assertSame(
            [['name' => 'site:code_copy', 'properties' => ['code_kind' => $kind]]],
            $this->capturedEvents($browser),
        );
    }
}
