<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

final class DbalMetadataTest extends FlowTestCase
{
    public function test_platform_options_drop_null_values(): void
    {
        static::assertEquals(
            Metadata::with('dbal_column_platform_options', ['charset' => 'utf8mb4']),
            DbalMetadata::platformOptions(['charset' => 'utf8mb4', 'collation' => null]),
        );
    }

    public function test_platform_options_of_only_nulls_is_empty_metadata(): void
    {
        static::assertEquals(Metadata::empty(), DbalMetadata::platformOptions([
            'charset' => null,
            'collation' => null,
        ]));
    }
}
