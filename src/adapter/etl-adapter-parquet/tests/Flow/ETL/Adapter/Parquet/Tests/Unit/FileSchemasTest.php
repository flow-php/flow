<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\FileSchemas;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Reader;
use Generator;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class FileSchemasTest extends FlowTestCase
{
    public static function files(string ...$names): Generator
    {
        $filesystem = new NativeLocalFilesystem();

        foreach ($names as $name) {
            $stream = $filesystem->readFrom(path(__DIR__ . '/../Integration/Fixtures/Pagination/partitioned/' . $name));

            yield ['file' => (new Reader())->readStream($stream), 'stream' => $stream];
        }
    }

    public function test_first_reads_only_the_first_file(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new FileSchemas(new SchemaConverter()))->first(
                self::files('date=2024-01-01/01_1000.parquet', 'date=2024-02-01/02_500.parquet'),
                [],
            ),
        );
    }

    public function test_first_narrows_to_the_requested_columns(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new FileSchemas(new SchemaConverter()))->first(self::files('date=2024-01-01/01_1000.parquet'), ['id']),
        );
    }

    public function test_first_of_an_empty_listing_is_an_empty_schema(): void
    {
        static::assertEquals(schema(), (new FileSchemas(new SchemaConverter()))->first(self::files(), []));
    }

    public function test_union_folds_every_file(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new FileSchemas(new SchemaConverter()))->union(
                self::files('date=2024-01-01/01_1000.parquet', 'date=2024-02-01/02_500.parquet'),
                [],
            ),
        );
    }

    public function test_union_narrows_to_the_requested_columns(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            (new FileSchemas(new SchemaConverter()))->union(
                self::files('date=2024-01-01/01_1000.parquet', 'date=2024-02-01/02_500.parquet'),
                ['name'],
            ),
        );
    }

    public function test_union_of_an_empty_listing_is_an_empty_schema(): void
    {
        static::assertEquals(schema(), (new FileSchemas(new SchemaConverter()))->union(self::files(), []));
    }
}
