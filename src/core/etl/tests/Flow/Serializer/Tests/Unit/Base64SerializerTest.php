<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry,
    datetime_entry,
    float_entry,
    int_entry,
    row,
    rows,
    str_entry,
    struct_entry,
    type_int,
    type_string,
    type_structure};
use Flow\ETL\{Row, Rows};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer};
use PHPUnit\Framework\TestCase;

final class Base64SerializerTest extends TestCase
{
    public function test_serializing_rows() : void
    {
        $rows = rows(
            ...\array_map(
                fn () : Row => row(
                    int_entry('integer', 1),
                    str_entry('string', 'string'),
                    bool_entry('boolean', true),
                    datetime_entry('datetime', new \DateTimeImmutable('2022-01-01 00:00:00')),
                    str_entry('null', null),
                    float_entry('float', 0.12),
                    struct_entry(
                        'struct',
                        ['integer' => 1, 'string' => 'string'],
                        type_structure([
                            'integer' => type_int(),
                            'string' => type_string(),
                        ])
                    )
                ),
                \range(0, 100)
            )
        );

        $serializer = new Base64Serializer(new NativePHPSerializer());

        $serialized = $serializer->serialize($rows);

        $unserialized = $serializer->unserialize($serialized, [Rows::class]);

        self::assertEquals(
            $rows,
            $unserialized
        );
    }
}
