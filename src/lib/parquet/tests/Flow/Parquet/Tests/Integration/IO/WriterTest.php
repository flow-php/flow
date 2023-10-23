<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

final class WriterTest extends ParquetIntegrationTestCase
{
    public function test_writer() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();

        $schema = Schema::with(
            FlatColumn::boolean('boolean'),
            FlatColumn::int32('int32'),
            FlatColumn::int64('int64'),
            FlatColumn::float('float'),
            FlatColumn::double('double'),
            FlatColumn::decimal('decimal'),
            FlatColumn::string('string'),
            NestedColumn::map('map_of_ints', MapKey::string(), MapValue::int32()),
            NestedColumn::list('list_of_strings', ListElement::string())
        );

        $writer->write($path, $schema, $inputData = [
            [
                'boolean' => true,
                'int32' => 32,
                'int64' => 64,
                'float' => 2.2,
                'double' => 2.2,
                'decimal' => 10.24,
                'string' => 'string',
                'map_of_ints' => [
                    'a' => 0,
                    'b' => 1,
                    'c' => 2,
                ],
                'list_of_strings' => ['string_00_00'],
            ],
            [
                'boolean' => false,
                'int32' => 150,
                'int64' => 64,
                'float' => 2.2,
                'double' => 2.2,
                'decimal' => 10.24,
                'string' => 'string',
                'map_of_ints' => [
                    'd' => 3,
                    'e' => 4,
                    'f' => 5,
                ],
                'list_of_strings' => ['string_01_00', 'string_01_01', 'string_01_02'],
            ],
        ]);

        $reader = new Reader();
        $file = $reader->read($path);

        $this->assertEquals(
            $inputData,
            \iterator_to_array($file->values())
        );
    }
}
