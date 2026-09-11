<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use FilesystemIterator;
use Flow\Website\Service\Examples\ExampleMeta;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function dirname;
use function Flow\Types\DSL\type_string;
use function mb_strlen;
use function mb_substr;
use function sort;
use function sprintf;

/**
 * The only thing that executes the published corpus. Readers meet it in the WASM playground, which
 * has its own autoloader, its own compiled extension set and no network, so that is where it runs.
 * Execution only - output is the framework's business, not the playground's.
 */
final class PlaygroundExamplesTest extends EndToEndTestCase
{
    public static function published_examples(): Generator
    {
        $root = dirname(__DIR__, 5) . '/content/examples/topics';
        /** @var iterable<SplFileInfo> $found */
        $found = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));
        $slugs = [];

        foreach ($found as $file) {
            if ($file->getFilename() !== 'code.php') {
                continue;
            }

            $directory = dirname($file->getPathname());

            if (ExampleMeta::fromDirectory($directory)->skipReason !== null) {
                continue;
            }

            $slugs[] = mb_substr($directory, mb_strlen($root) + 1);
        }

        sort($slugs);

        foreach ($slugs as $slug) {
            yield $slug => [$slug];
        }
    }

    #[DataProvider('published_examples')]
    public function test_a_published_example_runs_in_the_playground(string $slug): void
    {
        $browser = $this->openPlayground('/playground/' . $slug);

        $this->pageOf($browser)->evaluate('() => document.getElementById("action-run").click()');

        $browser->waitUntilVisible('[data-playground-output-target="container"] .output-message');

        $errors = type_string()->assert($this->pageOf($browser)->evaluate(
            '() => Array.from(document.querySelectorAll(\'[data-playground-output-target="container"] .output-error\')).map(e => e.textContent).join("\n")',
        ));

        static::assertSame('', $errors, sprintf('%s failed in the playground', $slug));
    }
}
