<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use PHPUnit\Framework\TestCase;

final class ArrayUnpackTransformerTest extends TestCase
{
    public function test_array_unpack_enum_with_schema() : void
    {
        $arrayUnpackTransformer = Transform::array_unpack(
            'array_entry',
            schema: new Row\Schema(Row\Schema\Definition::enum('enum', BackedIntEnum::class))
        );

        $rows = (Transform::remove('array_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(
                        new Row\Entry\ArrayEntry(
                            'array_entry',
                            [
                                'enum' => 'one',
                                'id' => 1,
                            ]
                        ),
                    ),
                ),
            )
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\EnumEntry('enum', BackedIntEnum::one),
                    new Row\Entry\IntegerEntry('id', 1)
                ),
            ),
            $rows
        );
    }

    public function test_array_unpack_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"integer_entry" is not ArrayEntry');

        $arrayUnpackTransformer = Transform::array_unpack('integer_entry');

        (Transform::remove('integer_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(
                        new Row\Entry\IntegerEntry('integer_entry', 1),
                    ),
                ),
            )
        );
    }

    public function test_array_unpack_for_not_existing_entry() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "array_entry" does not exist');

        $arrayUnpackTransformer = Transform::array_unpack('array_entry');

        (Transform::remove('integer_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(
                        new Row\Entry\IntegerEntry('integer_entry', 1),
                    ),
                ),
            )
        );
    }

    public function test_array_unpack_transformer() : void
    {
        $arrayUnpackTransformer = Transform::array_unpack('array_entry');

        $rows = (Transform::remove('array_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(
                        new Row\Entry\IntegerEntry('old_int', 1000),
                        new Row\Entry\ArrayEntry('array_entry', [
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'datetime' =>  new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                            'array' => ['foo', 'bar'],
                            'json' => '["foo", "bar"]',
                            'object' => new \stdClass(),
                            'null' => null,
                            'stringWithFloat' => '0.0',
                            'float' => 10.01,
                        ]),
                    ),
                ),
            )
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old_int', 1000),
                    new Row\Entry\IntegerEntry('id', 1),
                    new Row\Entry\StringEntry('status', 'PENDING'),
                    new Row\Entry\BooleanEntry('enabled', true),
                    new Row\Entry\DateTimeEntry('datetime', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\ListEntry('array', ScalarType::string, ['foo', 'bar']),
                    new Row\Entry\JsonEntry('json', ['foo', 'bar']),
                    new Row\Entry\ObjectEntry('object', new \stdClass()),
                    new Row\Entry\NullEntry('null'),
                    new Row\Entry\StringEntry('stringWithFloat', '0.0'),
                    new Row\Entry\FloatEntry('float', 10.01),
                ),
            ),
            $rows
        );
    }

    public function test_array_unpack_transformer_for_non_associative_array() : void
    {
        $arrayUnpackTransformer = Transform::array_unpack('array_entry');

        $rows = (Transform::remove('array_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(
                        new Row\Entry\ArrayEntry('array_entry', [
                            1,
                            'PENDING',
                            true,
                            new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                            ['foo', 'bar'],
                            '["foo", "bar"]',
                            new \stdClass(),
                            null,
                            0.25,
                        ]),
                    ),
                ),
            )
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('0', 1),
                    new Row\Entry\StringEntry('1', 'PENDING'),
                    new Row\Entry\BooleanEntry('2', true),
                    new Row\Entry\DateTimeEntry('3', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
                    new Row\Entry\ListEntry('4', ScalarType::string, ['foo', 'bar']),
                    new Row\Entry\JsonEntry('5', ['foo', 'bar']),
                    new Row\Entry\ObjectEntry('6', new \stdClass()),
                    new Row\Entry\NullEntry('7'),
                    new Row\Entry\FloatEntry('8', 0.25),
                ),
            ),
            $rows
        );
    }

    public function test_array_unpack_with_integer() : void
    {
        $arrayUnpackTransformer = Transform::array_unpack('array_entry');

        $rows = (Transform::remove('array_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(new Row\Entry\ArrayEntry('array_entry', ['id' => '1']), ),
                ),
            )
        );

        $this->assertEquals(
            new Rows(
                Row::create(new Row\Entry\StringEntry('id', '1')),
            ),
            $rows
        );
    }

    public function test_array_unpack_with_null_as_string() : void
    {
        $arrayUnpackTransformer = Transform::array_unpack('array_entry');

        $rows = (Transform::remove('array_entry'))->transform(
            $arrayUnpackTransformer->transform(
                new Rows(
                    Row::create(new Row\Entry\ArrayEntry('array_entry', ['status' => 'null']), ),
                ),
            )
        );

        $this->assertEquals(
            new Rows(
                Row::create(new Row\Entry\StringEntry('status', 'null')),
            ),
            $rows
        );
    }

    public function test_array_unpack_with_prefix() : void
    {
        $rows = (Transform::array_unpack('inventory', 'inventory_'))
            ->transform(
                new Rows(
                    Row::create(new Row\Entry\ArrayEntry('inventory', ['total' => 100, 'available' => 100, 'damaged' => 0]))
                )
            );

        $this->assertSame(
            [
                [
                    'inventory' => ['total' => 100, 'available' => 100, 'damaged' => 0],
                    'inventory_total' => 100,
                    'inventory_available' => 100,
                    'inventory_damaged' => 0,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_array_unpack_with_skipped_entries() : void
    {
        $rows = (Transform::array_unpack('inventory', '', ['available', 'damaged']))
            ->transform(
                new Rows(
                    Row::create(new Row\Entry\ArrayEntry('inventory', ['total' => 100, 'available' => 100, 'damaged' => 0]))
                )
            );

        $this->assertSame(
            [
                [
                    'inventory' => ['total' => 100, 'available' => 100, 'damaged' => 0],
                    'total' => 100,
                ],
            ],
            $rows->toArray()
        );
    }
}
