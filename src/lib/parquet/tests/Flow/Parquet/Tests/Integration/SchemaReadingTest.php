<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration;

use Flow\Parquet\Reader;

final class SchemaReadingTest extends ParquetIntegrationTestCase
{
    public function test_reading_lists_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $this->assertJsonStringEqualsJsonFile(
            __DIR__ . '/../Fixtures/lists.json',
            \json_encode(($reader->read(__DIR__ . '/../Fixtures/lists.parquet'))->metadata()->schema()->toDDL())
        );
    }

    public function test_reading_maps_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $this->assertJsonStringEqualsJsonFile(
            __DIR__ . '/../Fixtures/maps.json',
            \json_encode(($reader->read(__DIR__ . '/../Fixtures/maps.parquet'))->metadata()->schema()->toDDL())
        );
    }

    public function test_reading_primitives_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $this->assertJsonStringEqualsJsonFile(
            __DIR__ . '/../Fixtures/primitives.json',
            \json_encode(($reader->read(__DIR__ . '/../Fixtures/primitives.parquet'))->metadata()->schema()->toDDL())
        );
    }

    public function test_reading_structs_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $this->assertJsonStringEqualsJsonFile(
            __DIR__ . '/../Fixtures/structs.json',
            \json_encode(($reader->read(__DIR__ . '/../Fixtures/structs.parquet'))->metadata()->schema()->toDDL())
        );
    }
}
