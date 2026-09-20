<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\Filesystem\FileStatus;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class PartitionedSourceMother
{
    public static function partitions(): Schema
    {
        return schema(int_schema('year'), str_schema('month'));
    }

    /**
     * One row per partition. The double ignores the pushed path filter, so only a Filter node the planner kept can
     * drop the other partition's row.
     */
    public static function yearMonth(): RecordingFileExtractor
    {
        return (new RecordingFileExtractor(
            schema(int_schema('year'), str_schema('month'), str_schema('value')),
            rows(
                schema(int_schema('year'), str_schema('month'), str_schema('value')),
                row(['year' => 2023, 'month' => '07', 'value' => 'a']),
                row(['year' => 2024, 'month' => '08', 'value' => 'b']),
            ),
        ))->withPartitionSchema(self::partitions());
    }

    public static function file(string $partitions): FileStatus
    {
        return new FileStatus(path('flow-file://data/' . $partitions . '/file.csv'), true);
    }
}
