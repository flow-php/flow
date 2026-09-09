<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\Docs\DefinedFunctions;
use Flow\Documentation\Docs\DocumentationLinks;
use Flow\Documentation\Docs\DocumentDeclarations;
use Flow\Documentation\Docs\DocumentMethodCalls;
use Flow\Documentation\Docs\Fence;
use Flow\Documentation\Docs\FenceSymbols;
use Flow\Documentation\Docs\MarkdownFences;
use Flow\Documentation\Tests\Context\MarkdownCorpusContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_filter;
use function array_values;
use function implode;
use function sprintf;

final class DocumentationFencesTest extends TestCase
{
    /**
     * documentation/upgrading.md is append-only history. Its "Before:" fences are deliberately
     * written against APIs that no longer exist, so the whole file is excluded by name.
     */
    private const EXCLUDED = 'documentation/upgrading.md';

    public static function documentation_links(): Generator
    {
        $links = new DocumentationLinks();

        foreach ((new MarkdownCorpusContext())->filesIn(['documentation', 'web/landing/content']) as $file) {
            foreach ($links->of($file) as $link) {
                yield sprintf('%s:%d %s', $link['file'], $link['line'], $link['href']) => [$link['href']];
            }
        }
    }

    public static function documentation_pages(): Generator
    {
        foreach ((new MarkdownCorpusContext())->filesIn(['documentation']) as $file) {
            if ($file === self::EXCLUDED) {
                continue;
            }

            yield $file => [$file];
        }
    }

    public static function php_fences(): Generator
    {
        $fences = new MarkdownFences();

        foreach ((new MarkdownCorpusContext())->filesIn(['documentation']) as $file) {
            if ($file === self::EXCLUDED) {
                continue;
            }

            foreach ($fences->phpOf($file) as $fence) {
                if (!$fence->info->isIgnored()) {
                    yield $fence->location() => [$fence];
                }
            }
        }
    }

    #[DataProvider('documentation_links')]
    public function test_a_documentation_link_resolves(string $href): void
    {
        static::assertTrue((new DocumentationLinks())->resolves($href), sprintf('"%s" does not resolve.', $href));
    }

    #[DataProvider('php_fences')]
    public function test_a_php_fence_calls_only_functions_that_exist(Fence $fence): void
    {
        $unresolved = (new FenceSymbols())->unresolved(
            $fence,
            new DefinedFunctions(),
            new DocumentDeclarations((new MarkdownFences())->of($fence->file)),
        );

        static::assertSame(
            [],
            $unresolved,
            sprintf('%s calls undefined function(s): %s.', $fence->location(), implode(', ', $unresolved)),
        );
    }

    #[DataProvider('php_fences')]
    public function test_a_php_fence_tokenizes(Fence $fence): void
    {
        static::assertTrue(
            (new FenceSymbols())->tokenizes($fence),
            sprintf('%s does not parse. Mark it "```php ignore" if the fragment is deliberate.', $fence->location()),
        );
    }

    #[DataProvider('documentation_pages')]
    public function test_a_page_calls_only_methods_its_receivers_declare(string $file): void
    {
        $fences = array_values(array_filter(
            (new MarkdownFences())->phpOf($file),
            static fn(Fence $fence): bool => !$fence->info->isIgnored(),
        ));

        $messages = [];

        foreach ((new DocumentMethodCalls())->unresolved($fences) as $location => $calls) {
            $messages[] = $location . ': ' . implode(', ', $calls);
        }

        static::assertSame([], $messages, implode("\n", $messages));
    }
}
