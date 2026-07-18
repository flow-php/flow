<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Row;
use Flow\Serializer\Base64Serializer;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function range;

final class Base64SerializerTest extends TestCase
{
    public function test_serializing_rows(): void
    {
        $rows = rows(...array_map(
            static fn(): Row => row(
                int_entry('integer', 1),
                str_entry('string', 'string'),
                bool_entry('boolean', true),
                datetime_entry('datetime', new DateTimeImmutable('2022-01-01 00:00:00')),
                str_entry('null', null),
                float_entry('float', 0.12),
                struct_entry('struct', ['integer' => 1, 'string' => 'string'], type_structure([
                    'integer' => type_integer(),
                    'string' => type_string(),
                ])),
            ),
            range(0, 100),
        ));

        $serializer = new Base64Serializer(new NativePHPSerializer());

        $serialized = serialize_to_string($serializer, $rows);

        static::assertEquals($rows, unserialize_from_string($serializer, $serialized));
    }

    public function test_unserialize_of_invalid_base64_throws(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('failed to decode string');

        unserialize_from_string(new Base64Serializer(new NativePHPSerializer()), '@@@ not base64 @@@');
    }
}
