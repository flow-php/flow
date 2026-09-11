<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Row;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function range;
use function serialize;

final class NativePHPSerializerTest extends TestCase
{
    public function test_serializing_rows(): void
    {
        $rows = rows(
            schema(
                int_schema('integer'),
                str_schema('string'),
                bool_schema('boolean'),
                datetime_schema('datetime'),
                str_schema('null', nullable: true),
                float_schema('float'),
                structure_schema('struct', type_structure(['integer' => type_integer(), 'string' => type_string()])),
            ),
            ...array_map(
                static fn(): Row => row([
                    'integer' => 1,
                    'string' => 'string',
                    'boolean' => true,
                    'datetime' => new DateTimeImmutable('2022-01-01 00:00:00'),
                    'null' => null,
                    'float' => 0.12,
                    'struct' => ['integer' => 1, 'string' => 'string'],
                ]),
                range(0, 100),
            ),
        );

        $serializer = new NativePHPSerializer();

        $serialized = serialize_to_string($serializer, $rows);

        static::assertEquals($rows, unserialize_from_string($serializer, $serialized));
    }

    public function test_unserialize_of_non_rows_payload_throws(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('must return instance of');

        unserialize_from_string(new NativePHPSerializer(), serialize('just a string'));
    }
}
