<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;

final class PhpFloeEncoderTest extends TestCase
{
    public function test_decode_produces_row_values(): void
    {
        $original = array_to_rows(
            [['id' => 1, 'name' => 'flow', 'price' => 1.5], ['id' => 2, 'name' => null, 'price' => 0.5]],
            schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price')),
        );

        $encoder = new PhpFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($original->schema())));

        $decoded = $encoder->decode($encoder->encode($original));

        static::assertSame(['id' => 1, 'name' => 'flow', 'price' => 1.5], $decoded[0]->values);
        static::assertSame([], $decoded[0]->metadata);
        static::assertSame(['id' => 2, 'name' => null, 'price' => 0.5], $decoded[1]->values);
    }

    public function test_encode_round_trips(): void
    {
        $original = array_to_rows(
            [['id' => 1, 'name' => 'flow'], ['id' => 2, 'name' => null]],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        $encoder = new PhpFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($original->schema())));

        $decoded = $encoder->decode($encoder->encode($original));

        static::assertEquals(
            [
                new RawRowValues(['id' => 1, 'name' => 'flow']),
                new RawRowValues(['id' => 2, 'name' => null]),
            ],
            $decoded,
        );
    }

    public function test_encode_refuses_a_row_that_does_not_carry_a_declared_column(): void
    {
        // a row always carries every column its schema declares, so absence is a broken contract,
        // not a value the format can express - VALUE_ABSENT is a structure-element flag only
        $encoder = new PhpFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody(schema(
            int_schema('id'),
            str_schema('name'),
        ))));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe found a row that does not carry the declared column "name"');

        $encoder->encode(array_to_rows([['id' => 4]], schema(int_schema('id'))));
    }

    public function test_encode_decode_round_trip_preserves_flags(): void
    {
        // the column carries nulls, so the declaration admits them - a null on a NOT NULL column is
        // refused by the encoder
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        )));

        $encoder = new PhpFloeEncoder($schema);

        $encoded = [
            ...$encoder->encode(array_to_rows([['id' => 1, 'name' => 'flow'], ['id' => 2, 'name' => null]], $schema)),
            ...$encoder->encode(array_to_rows(
                [['id' => 3, 'name' => null]],
                $schema->setMetadata('name', Metadata::fromArray(['tag' => 'x'])),
            )),
        ];

        $decoded = $encoder->decode($encoded);

        static::assertEquals(
            [
                new RawRowValues(['id' => 1, 'name' => 'flow']),
                new RawRowValues(['id' => 2, 'name' => null]),
                new RawRowValues(['id' => 3, 'name' => null], ['name' => Metadata::fromArray(['tag' => 'x'])]),
            ],
            $decoded,
        );
        static::assertSame('x', $decoded[2]->metadata['name']->get('tag'));
    }

    public function test_encode_decode_round_trip_preserves_arbitrary_metadata(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(int_schema('id'), str_schema('name'))));

        $encoder = new PhpFloeEncoder($schema);

        $metadata = ['name' => Metadata::fromArray(['source' => 'trusted', 'weight' => 3])];

        $decoded = $encoder->decode($encoder->encode(array_to_rows(
            [['id' => 1, 'name' => 'flow']],
            $schema->setMetadata('name', $metadata['name']),
        )));

        static::assertEquals([new RawRowValues(['id' => 1, 'name' => 'flow'], $metadata)], $decoded);
        static::assertSame('trusted', $decoded[0]->metadata['name']->get('source'));
        static::assertSame(3, $decoded[0]->metadata['name']->get('weight'));
    }

    public function test_null_entry_in_a_typed_column_encodes_as_a_plain_null(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        )));

        $encoder = new PhpFloeEncoder($schema);
        $body = $encoder->encode(array_to_rows(
            [['id' => 2, 'name' => null]],
            schema(int_schema('id'), null_schema('name')),
        ))[0];

        $decoded = $encoder->decode([$body]);

        static::assertNull($decoded[0]->values['name']);
        static::assertArrayNotHasKey('name', $decoded[0]->metadata);
    }

    public function test_row_body_with_trailing_garbage_throws(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(int_schema('id'), str_schema('name'))));

        $encoder = new PhpFloeEncoder($schema);
        $body = $encoder->encode(array_to_rows(
            [['id' => 1, 'name' => 'flow']],
            schema(int_schema('id'), str_schema('name')),
        ))[0];

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe row frame length does not match its content');

        $encoder->decode([$body . "\x00"]);
    }

    public function test_unknown_value_flag_throws(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(int_schema('id'), str_schema('name'))));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown value flag');

        (new PhpFloeEncoder($schema))->decode(["\xEF"]);
    }

    public function test_decode_rows_builds_the_decoded_values(): void
    {
        $data = array_to_rows(
            [['id' => 1, 'name' => 'flow'], ['id' => 2, 'name' => null]],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );
        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $encoder = new PhpFloeEncoder($schema);
        $bodies = $encoder->encode($data);

        static::assertEquals(
            (new RowsBuilder($schema, new DefaultBackend()))
                ->appendRows(array_map(static fn(RawRowValues $r): array => $r->values, $encoder->decode($bodies)))
                ->finish(),
            $encoder->decodeRows($bodies, $schema),
        );
    }

    public function test_decode_rows_builds_through_the_given_backend(): void
    {
        $data = RowsMother::numbered(3);
        $backend = new SpyBackend();

        (new PhpFloeEncoder($data->schema(), $backend))->decodeRows(
            (new PhpFloeEncoder($data->schema()))->encode($data),
            $data->schema(),
        );

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    public function test_decode_rows_folds_metadata_onto_the_column_with_last_write_winning(): void
    {
        $plain = schema(int_schema('id', nullable: true));
        $encoder = new PhpFloeEncoder($plain);

        $bodies = [
            ...$encoder->encode(array_to_rows(
                [['id' => 1]],
                $plain->setMetadata('id', Metadata::fromArray(['k' => 'v1'])),
            )),
            ...$encoder->encode(array_to_rows(
                [['id' => 2]],
                $plain->setMetadata('id', Metadata::fromArray(['k' => 'v2'])),
            )),
        ];

        static::assertSame(
            ['k' => 'v2'],
            $encoder->decodeRows($bodies, $plain)->schema()->get('id')->metadata()->normalize(),
        );
    }

    public function test_decode_rows_folds_metadata_onto_a_numeric_column_name(): void
    {
        $plain = schema(int_schema('0', nullable: true));
        $encoder = new PhpFloeEncoder($plain);

        $bodies = $encoder->encode(array_to_rows(
            [['0' => 1]],
            $plain->setMetadata('0', Metadata::fromArray(['k' => 'v'])),
        ));

        static::assertSame(
            ['k' => 'v'],
            $encoder->decodeRows($bodies, $plain)->schema()->get('0')->metadata()->normalize(),
        );
    }

    public function test_decode_rows_ignores_metadata_of_an_undeclared_column(): void
    {
        $written = schema(int_schema('id'), int_schema('nope'));
        $encoder = new PhpFloeEncoder($written);

        $bodies = $encoder->encode(array_to_rows(
            [['id' => 1, 'nope' => 2]],
            $written->setMetadata('nope', Metadata::fromArray(['k' => 'v'])),
        ));

        static::assertEquals(
            schema(int_schema('id')),
            $encoder->decodeRows($bodies, schema(int_schema('id')))->schema(),
        );
    }

    public function test_encode_of_an_empty_batch_returns_no_bodies(): void
    {
        static::assertSame(
            [],
            (new PhpFloeEncoder(schema(int_schema('id'), str_schema('name'))))->encode(array_to_rows(
                [],
                schema(int_schema('id')),
            )),
        );
    }

    public function test_a_not_null_refusal_on_an_earlier_column_wins_over_a_missing_later_column(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0)');

        (new PhpFloeEncoder(schema(int_schema('id'), str_schema('name'))))->encode(array_to_rows([[
            'id' => null,
        ]], schema(int_schema('id', nullable: true))));
    }

    public function test_encode_frames_frames_the_encoded_rows(): void
    {
        $data = RowsMother::numbered(3);
        $encoder = new PhpFloeEncoder($data->schema());

        static::assertSame(Format::rowFrames($encoder->encode($data)), $encoder->encodeFrames($data));
    }
}
