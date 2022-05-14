<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech\Tests\Unit;

use Flow\ETL\Adapter\Avro\FlixTech\ValueConverter;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ValueConverterTest extends TestCase
{
    public function test_invalid_schema() : void
    {
        $this->expectExceptionMessage('Avro invalid schema provided');
        $this->expectException(InvalidArgumentException::class);

        new ValueConverter(['invalid_schema']);
    }

    public function test_simple_scalar_value() : void
    {
        $converter = new ValueConverter([
            'name' => 'row',
            'type' => 'record',
            'fields' => [
                ['name' => 'string_entry', 'type' => \AvroSchema::STRING_TYPE],
            ],
        ]);

        $this->assertSame(
            ['string_entry' => 'some_string'],
            $converter->convert(['string_entry' => 'some_string'])
        );
    }

    public function test_datetime_value() : void
    {
        $converter = new ValueConverter([
            'name' => 'row',
            'type' => 'record',
            'fields' => [
                ['name' => 'datetime_entry', 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'],
            ],
        ]);

        $this->assertEquals(
            ['datetime_entry' => new \DateTimeImmutable('2022-01-01 00:00:00')],
            $converter->convert(['datetime_entry' => (int) (new \DateTimeImmutable('2022-01-01 00:00:00'))->format('Uu')])
        );
    }

    public function test_list_of_ints() : void
    {
        $converter = new ValueConverter([
            'name' => 'row',
            'type' => 'record',
            'fields' => [
                ['name' => 'list_of_ints', 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
            ],
        ]);

        $this->assertSame(
            ['list_of_ints' => [1, 2, 3]],
            $converter->convert(['list_of_ints' => [1, 2, 3]])
        );
    }

    public function test_list_of_datetimes() : void
    {
        $converter = new ValueConverter([
            'name' => 'row',
            'type' => 'record',
            'fields' => [
                ['name' => 'list_of_datetimes', 'type' => ['type' => 'array', 'items' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros']],
            ],
        ]);

        $this->assertEquals(
            ['list_of_datetimes' => [new \DateTimeImmutable('2022-01-01 00:00:00'), new \DateTimeImmutable('2022-01-02 00:00:00')]],
            $converter->convert(['list_of_datetimes' => [
                (int) (new \DateTimeImmutable('2022-01-01 00:00:00'))->format('Uu'),
                (int) (new \DateTimeImmutable('2022-01-02 00:00:00'))->format('Uu'),
            ]])
        );
    }
}
