<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use FilesystemIterator;
use Flow\Website\Kernel;
use Generator;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

use function dirname;
use function mb_strlen;
use function mb_substr;
use function sprintf;

/**
 * A published example that 500s is invisible to the playground sweep, which only ever visits
 * /playground/. This walks the pages a reader actually opens.
 */
final class EveryExamplePageTest extends WebTestCase
{
    public static function example_pages(): Generator
    {
        $root = dirname(__DIR__, 5) . '/content/examples/topics';
        /** @var iterable<SplFileInfo> $found */
        $found = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));

        foreach ($found as $file) {
            if ($file->getFilename() === 'code.php') {
                $slug = mb_substr(dirname($file->getPathname()), mb_strlen($root) + 1);

                yield $slug => ['/' . $slug . '/'];
            }
        }
    }

    #[DataProvider('example_pages')]
    public function test_an_example_page_renders(string $url): void
    {
        self::createClient()->request('GET', $url);

        self::assertResponseIsSuccessful(sprintf('%s did not render', $url));
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
