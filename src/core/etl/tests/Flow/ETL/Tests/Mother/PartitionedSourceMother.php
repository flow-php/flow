<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\FileStatus;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class PartitionedSourceMother
{
    public static function partitions(): Schema
    {
        return schema(int_schema('year'), str_schema('month'));
    }

    public static function yearMonth(): RecordingFileExtractor
    {
        return (new RecordingFileExtractor(
            schema(int_schema('year'), str_schema('month'), str_schema('value')),
            array_to_rows(
                [['year' => 2023, 'month' => '07', 'value' => 'a'], ['year' => 2024, 'month' => '08', 'value' => 'b']],
                schema(int_schema('year'), str_schema('month'), str_schema('value')),
            ),
        ))->withPartitionSchema(self::partitions());
    }

    public static function file(string $partitions): FileStatus
    {
        return new FileStatus(path('flow-file://data/' . $partitions . '/file.csv'), true);
    }
}
