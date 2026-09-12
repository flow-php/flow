<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Context\GlobMatrixContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function sort;
use function strlen;
use function substr;

final class GlobListingAgreementTest extends TestCase
{
    public static function patterns(): Generator
    {
        yield from GlobMatrixContext::patterns();
    }

    protected function setUp(): void
    {
        foreach (GlobMatrixContext::files() as $file) {
            native_local_filesystem()
                ->writeTo(path(__DIR__ . '/var/glob_agreement/' . $file))
                ->append($file)
                ->close();
        }
    }

    protected function tearDown(): void
    {
        native_local_filesystem()->rm(path(__DIR__ . '/var/glob_agreement'));
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('patterns')]
    public function test_memory_filesystem_lists_the_matrix(string $pattern, array $expected): void
    {
        $memory = memory_filesystem();

        foreach (GlobMatrixContext::files() as $file) {
            $memory
                ->writeTo(path('memory://' . $file))
                ->append($file)
                ->close();
        }

        $listed = array_map(
            static fn(FileStatus $status): string => substr($status->path->uri(), strlen('memory://')),
            iterator_to_array($memory->list(path('memory://' . $pattern), new OnlyFiles()), false),
        );
        sort($listed);

        static::assertSame($expected, $listed);
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('patterns')]
    public function test_native_local_filesystem_lists_the_matrix(string $pattern, array $expected): void
    {
        $listed = array_map(
            static fn(FileStatus $status): string => substr(
                $status->path->path(),
                strlen(__DIR__ . '/var/glob_agreement/'),
            ),
            iterator_to_array(
                native_local_filesystem()->list(path(__DIR__ . '/var/glob_agreement/' . $pattern), new OnlyFiles()),
                false,
            ),
        );
        sort($listed);

        static::assertSame($expected, $listed);
    }
}
