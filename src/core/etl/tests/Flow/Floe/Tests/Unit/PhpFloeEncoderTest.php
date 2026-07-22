<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class PhpFloeEncoderTest extends TestCase
{
    public function test_decode_produces_row_values(): void
    {
        $original = rows(
            row(int_entry('id', 1), str_entry('name', 'flow'), float_entry('price', 1.5)),
            row(int_entry('id', 2), str_entry('name', null), float_entry('price', 0.5)),
        );

        $encoder = new PhpFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($original->first()->schema())));

        $decoded = $encoder->decode($encoder->encode((new PhpRowHydrator())->dehydrate($original)));

        static::assertSame(['id' => 1, 'name' => 'flow', 'price' => 1.5], $decoded[0]->values);
        static::assertSame([], $decoded[0]->metadata);
        static::assertSame(['id' => 2, 'name' => null, 'price' => 0.5], $decoded[1]->values);
    }

    public function test_encode_from_dehydrated_rows_round_trips(): void
    {
        $original = rows(
            row(int_entry('id', 1), str_entry('name', 'flow')),
            row(int_entry('id', 2), str_entry('name', null)),
        );

        $encoder = new PhpFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($original->first()->schema())));

        $decoded = $encoder->decode($encoder->encode((new PhpRowHydrator())->dehydrate($original)));

        static::assertEquals(
            [
                new RawRowValues(['id' => 1, 'name' => 'flow']),
                new RawRowValues(['id' => 2, 'name' => null]),
            ],
            $decoded,
        );
    }

    public function test_encode_decode_round_trip_preserves_flags(): void
    {
        $schema = schema_from_json(
            FloeSchemaContext::schemaBody(row(int_entry('id', 1), str_entry('name', 'flow'))->schema()),
        );

        $encoder = new PhpFloeEncoder($schema);

        $types = ['id' => type_integer(), 'name' => type_string()];
        $encoded = [
            new TypedRowValues(['id' => 1, 'name' => 'flow'], $types),
            new TypedRowValues(['id' => 2, 'name' => null], $types),
            new TypedRowValues(['id' => 3, 'name' => null], $types, ['name' => Metadata::fromArray(['tag' => 'x'])]),
            new TypedRowValues(['id' => 4], ['id' => type_integer()]),
        ];

        $decoded = $encoder->decode($encoder->encode($encoded));

        static::assertEquals(
            [
                new RawRowValues(['id' => 1, 'name' => 'flow']),
                new RawRowValues(['id' => 2, 'name' => null]),
                new RawRowValues(['id' => 3, 'name' => null], ['name' => Metadata::fromArray(['tag' => 'x'])]),
                new RawRowValues(['id' => 4]),
            ],
            $decoded,
        );
        static::assertSame('x', $decoded[2]->metadata['name']->get('tag'));
        static::assertArrayNotHasKey('name', $decoded[3]->values);
    }

    public function test_encode_decode_round_trip_preserves_arbitrary_metadata(): void
    {
        $schema = schema_from_json(
            FloeSchemaContext::schemaBody(row(int_entry('id', 1), str_entry('name', 'flow'))->schema()),
        );

        $encoder = new PhpFloeEncoder($schema);

        $metadata = ['name' => Metadata::fromArray(['source' => 'trusted', 'weight' => 3])];
        $encoded = [
            new TypedRowValues(
                ['id' => 1, 'name' => 'flow'],
                ['id' => type_integer(), 'name' => type_string()],
                $metadata,
            ),
        ];

        $decoded = $encoder->decode($encoder->encode($encoded));

        static::assertEquals([new RawRowValues(['id' => 1, 'name' => 'flow'], $metadata)], $decoded);
        static::assertSame('trusted', $decoded[0]->metadata['name']->get('source'));
        static::assertSame(3, $decoded[0]->metadata['name']->get('weight'));
    }

    public function test_null_entry_in_a_typed_column_encodes_as_a_plain_null(): void
    {
        $schema = schema_from_json(
            FloeSchemaContext::schemaBody(row(int_entry('id', 1), str_entry('name', 'x'))->schema()),
        );

        $encoder = new PhpFloeEncoder($schema);
        $body = $encoder->encode((new PhpRowHydrator())->dehydrate(rows(row(
            int_entry('id', 2),
            null_entry('name'),
        ))))[0];

        $decoded = $encoder->decode([$body]);

        static::assertNull($decoded[0]->values['name']);
        static::assertArrayNotHasKey('name', $decoded[0]->metadata);
    }

    public function test_row_body_with_trailing_garbage_throws(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(row(int_entry('id', 1))->schema()));

        $encoder = new PhpFloeEncoder($schema);
        $body = $encoder->encode((new PhpRowHydrator())->dehydrate(rows(row(int_entry('id', 1)))))[0];

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe row frame length does not match its content');

        $encoder->decode([$body . "\x00"]);
    }

    public function test_unknown_value_flag_throws(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(row(int_entry('id', 1))->schema()));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown value flag');

        (new PhpFloeEncoder($schema))->decode(["\xEF"]);
    }
}
