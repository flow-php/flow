<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;

use function iterator_to_array;

class WriterValidatorTest extends ParquetIntegrationTestCase
{
    public function test_writing_int_value_to_string_column(): void
    {
        $this->expectExceptionMessage('Column "string" is not string, got "integer" instead');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::string('string'));

        $writer->write($path, $schema, [['string' => 12345]]);
    }

    public function test_writing_null_to_list_that_is_required(): void
    {
        $this->expectExceptionMessage('Column "list" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(NestedColumn::list('list', ListElement::string())->makeRequired());

        $writer->write($path, $schema, [['list' => null]]);
    }

    public function test_writing_null_to_list_with_element_is_required(): void
    {
        $this->expectExceptionMessage('Column "list.list.element" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(NestedColumn::list('list', ListElement::string(required: true)));

        $writer->write($path, $schema, [['list' => [null]]]);
    }

    public function test_writing_null_to_map_with_value_required(): void
    {
        $this->expectExceptionMessage('Column "map.key_value.value" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(NestedColumn::map('map', MapKey::string(), MapValue::string(required: true)));

        $writer->write($path, $schema, [['map' => ['a' => null]]]);
    }

    public function test_writing_null_to_required_map(): void
    {
        $this->expectExceptionMessage('Column "map" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(NestedColumn::map('map', MapKey::string(), MapValue::string())->makeRequired());

        $writer->write($path, $schema, [['map' => null]]);
    }

    public function test_writing_null_value_to_required_column(): void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::string('string')->makeRequired());

        $writer->write($path, $schema, [['string' => null]]);
    }

    public function test_writing_row_with_missing_optional_columns(): void
    {
        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id'), FlatColumn::string('string'));

        $writer->write($path, $schema, [['id' => 123], []]);

        static::assertFileExists($path);

        $reader = new Reader();
        $file = $reader->read($path);

        static::assertSame(
            [
                [
                    'id' => 123,
                    'string' => null,
                ],
                [
                    'id' => null,
                    'string' => null,
                ],
            ],
            iterator_to_array($file->values()),
        );
    }

    public function test_writing_row_with_missing_optional_columns_in_different_columns(): void
    {
        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id'), FlatColumn::string('string'));

        $writer->write($path, $schema, [
            ['id' => 123],
            ['string' => 'string'],
            ['id' => 123, 'string' => 'string'],
            ['id' => 123, 'string' => null],
            ['id' => null, 'string' => 'string'],
        ]);

        $reader = new Reader();
        $file = $reader->read($path);

        static::assertSame(
            [
                ['id' => 123, 'string' => null],
                ['id' => null, 'string' => 'string'],
                ['id' => 123, 'string' => 'string'],
                ['id' => 123, 'string' => null],
                ['id' => null, 'string' => 'string'],
            ],
            iterator_to_array($file->values()),
        );
    }

    public function test_writing_row_without_required_column(): void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = Writer::php();
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id'), FlatColumn::string('string')->makeRequired());

        $writer->write($path, $schema, [['id' => 123]]);
    }
}
