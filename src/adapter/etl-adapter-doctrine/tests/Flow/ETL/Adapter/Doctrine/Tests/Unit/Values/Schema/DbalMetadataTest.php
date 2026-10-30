<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit\Values\Schema;

use Flow\ETL\Adapter\Doctrine\Values\Schema\DbalMetadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Metadata;

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
