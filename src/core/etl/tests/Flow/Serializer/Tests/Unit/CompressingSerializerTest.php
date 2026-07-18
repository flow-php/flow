<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Row;
use Flow\Serializer\CompressingSerializer;
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
use function function_exists;
use function range;

final class CompressingSerializerTest extends TestCase
{
    protected function setUp(): void
    {
        if (!function_exists('gzcompress')) {
            self::markTestSkipped('gzcompress unavailable.');
        }
    }

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

        $serializer = new CompressingSerializer(new NativePHPSerializer());

        $serialized = serialize_to_string($serializer, $rows);

        static::assertEquals($rows, unserialize_from_string($serializer, $serialized));
    }
}
