<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Kernels\Sort\ExternalSort;

use Composer\Autoload\ClassLoader;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function array_filter;
use function array_key_first;
use function fclose;
use function proc_close;
use function proc_open;
use function stream_get_contents;

use const PHP_BINARY;

final class ExternalSortMemoryLimitTest extends FlowIntegrationTestCase
{
    #[TestWith(['200000', 'flat'])]
    #[TestWith(['60000', 'list'])]
    public function test_a_sort_of_more_rows_than_the_memory_limit_holds_twice_finishes(
        string $rows,
        string $shape,
    ): void {
        $pipes = [];
        // a child process: a sort that runs out of memory ends PHP, not only the test
        $process = proc_open(
            [
                PHP_BINARY,
                '-d',
                'memory_limit=64M',
                __DIR__ . '/../../../../Fixtures/MemoryLimit/sort.php',
                // the project's autoloader, not the one of the tool that runs the tests
                (string) array_key_first(array_filter(
                    ClassLoader::getRegisteredLoaders(),
                    static fn(ClassLoader $loader): bool => $loader->findFile(self::class) !== false,
                )) . '/autoload.php',
                $rows,
                $shape,
            ],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        static::assertNotFalse($process);
        /** @var array{1: resource, 2: resource} $pipes */
        $output = (string) stream_get_contents($pipes[1]);
        $errors = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        static::assertSame(0, proc_close($process), $output . $errors);
        static::assertSame($rows, $output);
    }
}
