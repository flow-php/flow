<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class NativePHPSerializerTest extends TestCase
{
    public function test_serializing_rows() : void
    {
        $rows = new Rows(
            ...\array_map(
                fn () : Row => Row::create(
                    int_entry('integer', 1),
                    str_entry('string', 'string'),
                    bool_entry('boolean', true),
                    datetime_entry('datetime', new \DateTimeImmutable('2022-01-01 00:00:00')),
                    null_entry('null'),
                    float_entry('float', 0.12),
                    object_entry('object', new \ArrayIterator([1, 2, 3])),
                    struct_entry(
                        'struct',
                        ['integer' => 1, 'string' => 'string'],
                        struct_type([
                            struct_element('integer', type_int()),
                            struct_element('string', type_string()),
                        ])
                    )
                ),
                \range(0, 100)
            )
        );

        $serializer = new NativePHPSerializer();

        $serialized = $serializer->serialize($rows);

        $this->assertEquals(
            $rows,
            $serializer->unserialize($serialized, Rows::class)
        );
    }
}
