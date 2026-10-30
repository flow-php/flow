<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\File\FileMetadataRow;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;

use function Flow\Filesystem\DSL\path;

final class FileMetadataRowTest extends FlowTestCase
{
    public function test_a_file_is_described_by_its_path_names_kind_and_extension(): void
    {
        static::assertSame(
            [
                'path' => '/dir/a.csv',
                'protocol' => 'memory',
                'file_name' => 'a',
                'base_name' => 'a.csv',
                'is_file' => true,
                'is_dir' => false,
                'extension' => 'csv',
            ],
            (new FileMetadataRow())->row(new FileStatus(path('memory://dir/a.csv'), true)),
        );
    }

    public function test_a_file_without_an_extension_has_a_null_extension(): void
    {
        static::assertNull((new FileMetadataRow())->row(new FileStatus(path('memory://dir/a'), true))['extension']);
    }
}
