<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use FilesystemIterator;
use Flow\Website\Service\Examples\ExampleMeta;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function dirname;
use function mb_strlen;
use function mb_substr;

final class ExampleCorpusMetaTest extends TestCase
{
    public static function example_directories(): Generator
    {
        $root = dirname(__DIR__, 5) . '/content/examples/topics';
        /** @var iterable<SplFileInfo> $found */
        $found = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));

        foreach ($found as $file) {
            if ($file->getFilename() === '_meta.yaml') {
                $directory = dirname($file->getPathname());

                yield mb_substr($directory, mb_strlen($root) + 1) => [$directory];
            }
        }
    }

    #[DataProvider('example_directories')]
    public function test_every_meta_file_in_the_corpus_is_valid(string $directory): void
    {
        static::assertNotSame(99, ExampleMeta::fromDirectory($directory)->priority);
    }
}
