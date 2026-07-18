<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlEncoder;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class PostgreSqlEncoderTest extends FlowTestCase
{
    public function test_decode_wraps_each_value_map_in_row_values(): void
    {
        $decoded = (new PostgreSqlEncoder())->decode([
            ['id' => 1, 'name' => 'Norbert'],
            ['id' => 2, 'name' => null],
        ]);

        static::assertEquals(
            [new RawRowValues(['id' => 1, 'name' => 'Norbert']), new RawRowValues(['id' => 2, 'name' => null])],
            $decoded,
        );
    }

    public function test_encode_unwraps_each_typed_row_values_to_its_value_map(): void
    {
        $types = ['id' => type_integer(), 'name' => type_string()];

        $encoded = (new PostgreSqlEncoder())->encode([
            new TypedRowValues(['id' => 1, 'name' => 'Norbert'], $types),
            new TypedRowValues(['id' => 2, 'name' => null], $types),
        ]);

        static::assertSame([['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => null]], $encoded);
    }

    public function test_encode_returns_the_original_value_maps(): void
    {
        $types = ['id' => type_integer(), 'active' => type_boolean()];

        static::assertSame(
            [['id' => 1, 'active' => true], ['id' => 2, 'active' => false]],
            (new PostgreSqlEncoder())->encode([
                new TypedRowValues(['id' => 1, 'active' => true], $types),
                new TypedRowValues(['id' => 2, 'active' => false], $types),
            ]),
        );
    }

    public function test_encode_and_decode_of_an_empty_batch_return_empty(): void
    {
        $encoder = new PostgreSqlEncoder();

        static::assertSame([], $encoder->decode([]));
        static::assertSame([], $encoder->encode([]));
    }
}
