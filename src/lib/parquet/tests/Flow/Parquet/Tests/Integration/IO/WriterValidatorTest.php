<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema;
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

    public function test_skipping_required_row() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(
            Schema\FlatColumn::int32('id'),
            Schema\FlatColumn::string('string')->makeRequired()
        );

        $writer->write($path, $schema, [['id' => 123]]);
    }

    public function test_writing_int_value_to_string_column() : void
    {
        $this->expectExceptionMessage('Column "string" is not string, got "integer" instead');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\FlatColumn::string('string'));

        $writer->write($path, $schema, [['string' => 12345]]);
    }

    public function test_writing_null_to_list_that_is_required() : void
    {
        $this->expectExceptionMessage('Column "list" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::list('list', Schema\ListElement::string())->makeRequired());

        $writer->write($path, $schema, [['list' => null]]);
    }

    public function test_writing_null_to_list_with_element_is_required() : void
    {
        $this->expectExceptionMessage('Column "list.list.element" is not string, got "NULL" instead');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::list('list', Schema\ListElement::string(required: true)));

        $writer->write($path, $schema, [['list' => [null]]]);
    }

    public function test_writing_null_to_map_with_value_required() : void
    {
        $this->expectExceptionMessage('Column "map.key_value.value" is not string, got "NULL" instead');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::map('map', Schema\MapKey::string(), Schema\MapValue::string(required: true)));

        $writer->write($path, $schema, [['map' => ['a' => null]]]);
    }

    public function test_writing_null_to_required_map() : void
    {
        $this->expectExceptionMessage('Column "map" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::map('map', Schema\MapKey::string(), Schema\MapValue::string())->makeRequired());

        $writer->write($path, $schema, [['map' => null]]);
    }

    public function test_writing_null_value_to_required_column() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(Schema\FlatColumn::string('string')->makeRequired());

        $writer->write($path, $schema, [['string' => null]]);
    }

    public function test_writing_row_with_missing_optional_columns() : void
    {
        $writer = new Writer();
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(
            Schema\FlatColumn::int32('id'),
            Schema\FlatColumn::string('string')
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
        $path = __DIR__ . '/var/test-writer-validator-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with(
            Schema\FlatColumn::int32('id'),
            Schema\FlatColumn::string('string')
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
}
