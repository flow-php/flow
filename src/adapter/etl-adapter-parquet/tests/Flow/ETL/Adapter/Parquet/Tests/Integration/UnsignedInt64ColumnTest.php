<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_numeric_string;

final class UnsignedInt64ColumnTest extends FlowTestCase
{
    public function test_a_uint64_column_reads_as_numeric_strings_typed_numeric_string(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/uint64.parquet');
        $writer = new Writer(engine: new PhpParquetEngine());
        $writer->openForStream(
            $memory->writeTo($path),
            Schema::with(new FlatColumn('id', PhysicalType::INT64, logicalType: LogicalType::integer(64, false))),
        );
        $writer->writeBatch([['id' => 0], ['id' => '9223372036854775808'], ['id' => '18446744073709551615']]);
        $writer->close();

        $df = data_frame()->read(from_parquet($path, filesystem: $memory));
        $rows = $df->fetch();

        static::assertTrue(type_equals(type_numeric_string(), $df->schema()->get('id')->type()));
        static::assertTrue(type_equals(type_numeric_string(), $rows->schema()->get('id')->type()));
        static::assertTrue(type_equals(type_numeric_string(), $rows->column('id')->type()));
        static::assertSame(['0', '9223372036854775808', '18446744073709551615'], $rows->column('id')->values());
    }
}
