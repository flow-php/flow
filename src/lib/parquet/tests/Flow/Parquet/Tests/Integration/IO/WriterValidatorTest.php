<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

final class WriterValidatorTest extends TestCase
{
    public function test_skipping_required_row() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

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
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\FlatColumn::string('string'));

        $writer->write($path, $schema, [['string' => 12345]]);
    }

    public function test_writing_null_to_list_that_is_required() : void
    {
        $this->expectExceptionMessage('Column "list" is required');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::list('list', Schema\ListElement::string())->makeRequired());

        $writer->write($path, $schema, [['list' => null]]);
    }

    public function test_writing_null_to_list_with_element_is_required() : void
    {
        $this->expectExceptionMessage('Column "list.list.element" is not string, got "NULL" instead');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::list('list', Schema\ListElement::string(required: true)));

        $writer->write($path, $schema, [['list' => [null]]]);
    }

    public function test_writing_null_to_map_with_value_required() : void
    {
        $this->expectExceptionMessage('Column "map.key_value.value" is not string, got "NULL" instead');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::map('map', Schema\MapKey::string(), Schema\MapValue::string(required: true)));

        $writer->write($path, $schema, [['map' => ['a' => null]]]);
    }

    public function test_writing_null_to_required_map() : void
    {
        $this->expectExceptionMessage('Column "map" is required');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\NestedColumn::map('map', Schema\MapKey::string(), Schema\MapValue::string())->makeRequired());

        $writer->write($path, $schema, [['map' => null]]);
    }

    public function test_writing_null_value_to_required_column() : void
    {
        $this->expectExceptionMessage('Column "string" is required');

        $writer = new Writer();
        $path = \sys_get_temp_dir() . '/test-writer-validator' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = Schema::with(Schema\FlatColumn::string('string')->makeRequired());

        $writer->write($path, $schema, [['string' => null]]);
    }
}
