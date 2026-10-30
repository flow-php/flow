<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Filesystem;

use Flow\ETL\IO\Filesystem\VisibleFiles;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\path;

final class VisibleFilesTest extends FlowTestCase
{
    #[TestWith(['file:///out/.flow-tmp-1a2b/00000/part.csv'])]
    #[TestWith(['file:///out/_temporary/part.csv'])]
    #[TestWith(['file:///out/.part.csv'])]
    #[TestWith(['file:///out/_SUCCESS'])]
    #[TestWith(['file:///out/year=2024/.hidden/part.csv'])]
    public function test_a_hidden_segment_below_the_root_hides_the_file(string $file): void
    {
        static::assertFalse((new VisibleFiles(path('file:///out')))->accept(new FileStatus(path($file), true)));
    }

    #[TestWith(['file:///out/part.csv'])]
    #[TestWith(['file:///out/_year=2024/part.csv'])]
    #[TestWith(['file:///out/year=_2024/part.csv'])]
    #[TestWith(['file:///out/_metadata'])]
    #[TestWith(['file:///out/_common_metadata'])]
    #[TestWith(['file:///out/a.b/part.csv'])]
    public function test_a_visible_file_below_the_root_is_kept(string $file): void
    {
        static::assertTrue((new VisibleFiles(path('file:///out')))->accept(new FileStatus(path($file), true)));
    }

    public function test_a_hidden_segment_of_the_root_itself_is_read(): void
    {
        static::assertTrue((new VisibleFiles(path('file:///home/u/.local/_exports')))->accept(
            new FileStatus(path('file:///home/u/.local/_exports/part.csv'), true),
        ));
    }

    public function test_an_explicit_hidden_file_is_read(): void
    {
        static::assertTrue((new VisibleFiles(path('file:///out/.hidden.csv')))->accept(
            new FileStatus(path('file:///out/.hidden.csv'), true),
        ));
    }

    public function test_a_root_prefix_is_not_a_parent(): void
    {
        static::assertTrue((new VisibleFiles(path('file:///out')))->accept(
            new FileStatus(path('file:///out.csv'), true),
        ));
    }

    public function test_a_directory_is_not_a_file(): void
    {
        static::assertFalse((new VisibleFiles(path('file:///out')))->accept(
            new FileStatus(path('file:///out/a'), false),
        ));
    }
}
