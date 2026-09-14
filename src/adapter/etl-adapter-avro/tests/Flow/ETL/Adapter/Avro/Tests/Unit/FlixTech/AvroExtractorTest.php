<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Unit\FlixTech;

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Tests\FlowTestCase;
use ReflectionClass;

use function Flow\ETL\DSL\schema;

final class AvroExtractorTest extends FlowTestCase
{
    public function test_it_declares_no_partition_columns(): void
    {
        // the constructor always throws, so only an instance built without it can be asked
        static::assertEquals(
            schema(),
            (new ReflectionClass(AvroExtractor::class))->newInstanceWithoutConstructor()->partitionSchema(),
        );
    }
}
