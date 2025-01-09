<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\generate_random_string;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use Flow\Parquet\{Reader, Writer};
use PHPUnit\Framework\TestCase;

final class WriterValidatorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_writing_int_value_to_string_column() : void
    {
        $this->expectExceptionMessage('Column "string" is not string, got "integer" instead');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::string('string'));

        $writer->write($path, $schema, [['string' => 12345]]);
    }

    public function test_writing_null_to_list_that_is_required() : void
    {
        $this->expectExceptionMessage('Column "list" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(NestedColumn::list('list', ListElement::string())->makeRequired());

        $writer->write($path, $schema, [['list' => null]]);
    }

    public function test_writing_null_to_list_with_element_is_required() : void
    {
        $this->expectExceptionMessage('Column "list.list.element" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(NestedColumn::list('list', ListElement::string(required: true)));

        $writer->write($path, $schema, [['list' => [null]]]);
    }

    public function test_writing_null_to_map_with_value_required() : void
    {
        $this->expectExceptionMessage('Column "map.key_value.value" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(NestedColumn::map('map', MapKey::string(), MapValue::string(required: true)));

        $writer->write($path, $schema, [['map' => ['a' => null]]]);
    }

    public function test_writing_null_to_required_map() : void
    {
        $this->expectExceptionMessage('Column "map" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(NestedColumn::map('map', MapKey::string(), MapValue::string())->makeRequired());

        $writer->write($path, $schema, [['map' => null]]);
    }

    public function test_writing_null_value_to_required_column() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::string('string')->makeRequired());

        $writer->write($path, $schema, [['string' => null]]);
    }

    public function test_writing_row_with_missing_optional_columns() : void
    {
        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('string')
        );

        $writer->write($path, $schema, [['id' => 123], []]);

        self::assertFileExists($path);

        $reader = new Reader();
        $file = $reader->read($path);

        self::assertSame(
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
            \iterator_to_array($file->values())
        );

        \unlink($path);
    }

    public function test_writing_row_with_missing_optional_columns_in_different_columns() : void
    {
        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('string')
        );

        $writer->write($path, $schema, [
            ['id' => 123],
            ['string' => 'string'],
            ['id' => 123, 'string' => 'string'],
            ['id' => 123, 'string' => null],
            ['id' => null, 'string' => 'string'],
        ]);

        $reader = new Reader();
        $file = $reader->read($path);

        self::assertSame(
            [
                ['id' => 123, 'string' => null],
                ['id' => null, 'string' => 'string'],
                ['id' => 123, 'string' => 'string'],
                ['id' => 123, 'string' => null],
                ['id' => null, 'string' => 'string'],
            ],
            \iterator_to_array($file->values())
        );

        unlink($path);
    }

    public function test_writing_row_without_required_column() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('string')->makeRequired()
        );

        $writer->write($path, $schema, [['id' => 123]]);
    }
}
