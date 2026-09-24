<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\Tests\Double\SpyHydrator;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class NativeFloeEncoderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension with RustFloeEncoderNative is not loaded');
        }
    }

    public function test_native_encode_matches_the_php_engine(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(int_schema('id'), str_schema('name'))));

        $encoded = [new TypedRowValues(['id' => 1, 'name' => 'flow'], [
            'id' => type_integer(),
            'name' => type_string(),
        ])];

        static::assertSame(
            (new PhpFloeEncoder($schema))->encode($encoded),
            (new NativeFloeEncoder($schema))->encode($encoded),
        );
    }

    public function test_native_decode_matches_the_php_engine(): void
    {
        $data = rows(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            row(['id' => 1, 'name' => 'flow']),
            row(['id' => 2, 'name' => null]),
        );

        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $bodies = (new PhpFloeEncoder($schema))->encode((new PhpRowHydrator())->dehydrate($data));

        static::assertEquals(
            (new PhpFloeEncoder($schema))->decode($bodies),
            (new NativeFloeEncoder($schema))->decode($bodies),
        );
    }

    public function test_native_metadata_bearing_frames_match_the_php_engine(): void
    {
        $schema = schema_from_json(FloeSchemaContext::schemaBody(schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        )));

        $encoded = [new TypedRowValues(
            ['id' => 2, 'name' => null],
            ['id' => type_integer(), 'name' => type_string()],
            ['id' => Metadata::fromArray(['source' => 'trusted', 'weight' => 3])],
        )];

        $bodies = (new PhpFloeEncoder($schema))->encode($encoded);

        static::assertEquals(
            (new PhpFloeEncoder($schema))->decode($bodies),
            (new NativeFloeEncoder($schema))->decode($bodies),
        );
        static::assertSame(
            (new PhpFloeEncoder($schema))->encode($encoded),
            (new NativeFloeEncoder($schema))->encode($encoded),
        );
    }

    /**
     * Flags, not instances: PHPUnit builds provider data before setUp() skips, and a native hydrator cannot be
     * constructed without the extension.
     *
     * @return array<string, array{bool}>
     */
    public static function native_hydrators(): array
    {
        return [
            'native' => [false],
            'adaptive' => [true],
        ];
    }

    #[DataProvider('native_hydrators')]
    public function test_decode_rows_matches_hydrate_of_decode(bool $adaptive): void
    {
        $data = rows(
            schema(int_schema('id'), str_schema('name', nullable: true), datetime_schema('at')),
            row([
                'id' => 1,
                'name' => 'flow',
                'at' => new DateTimeImmutable('2026-01-01 10:00:00.5', new DateTimeZone('Europe/Warsaw')),
            ]),
            row(['id' => 2, 'name' => null, 'at' => new DateTimeImmutable('2026-01-02T00:00:00Z')]),
        );
        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $encoder = new NativeFloeEncoder($schema);
        $bodies = $encoder->encode((new PhpRowHydrator())->dehydrate($data));

        static::assertEquals(
            (new NativeRowHydrator())->hydrate($encoder->decode($bodies), $schema),
            $encoder->decodeRows($bodies, $schema, $adaptive ? new AdaptiveRowHydrator() : new NativeRowHydrator()),
        );
    }

    public function test_decode_rows_hands_a_non_native_hydrator_the_decoded_values(): void
    {
        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $encoder = new NativeFloeEncoder($schema);
        $bodies = $encoder->encode((new PhpRowHydrator())->dehydrate($data));
        $spy = new SpyHydrator();

        static::assertEquals(
            (new PhpRowHydrator())->hydrate($encoder->decode($bodies), $schema),
            $encoder->decodeRows($bodies, $schema, $spy),
        );
        static::assertSame(1, $spy->hydrateCalls);
    }

    public function test_decode_rows_throws_the_schema_mismatch_the_hydrator_throws(): void
    {
        $data = rows(schema(int_schema('id')), row(['id' => 1]));
        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $encoder = new NativeFloeEncoder($schema);
        $bodies = $encoder->encode((new PhpRowHydrator())->dehydrate($data));

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "name" (row 0) declared by the schema is missing from the row');

        $encoder->decodeRows($bodies, schema(int_schema('id'), str_schema('name')), new NativeRowHydrator());
    }

    public function test_decode_rows_turns_a_corrupt_frame_into_a_floe_exception(): void
    {
        $data = rows(schema(int_schema('id')), row(['id' => 1]));
        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->schema()));
        $encoder = new NativeFloeEncoder($schema);
        $bodies = $encoder->encode((new PhpRowHydrator())->dehydrate($data));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('flow_php row frame length does not match its content');

        $encoder->decodeRows([$bodies[0] . "\xEF"], $schema, new NativeRowHydrator());
    }

    #[DataProvider('native_hydrators')]
    public function test_encode_frames_matches_framing_encode_of_dehydrate(bool $adaptive): void
    {
        $data = RowsMother::numbered(4);
        $encoder = new NativeFloeEncoder($data->schema());

        static::assertSame(
            Format::rowFrames($encoder->encode((new PhpRowHydrator())->dehydrate($data))),
            $encoder->encodeFrames($data, $adaptive ? new AdaptiveRowHydrator() : new NativeRowHydrator()),
        );
    }

    public function test_encode_frames_hands_a_non_native_hydrator_the_rows(): void
    {
        $data = RowsMother::numbered(2);
        $encoder = new NativeFloeEncoder($data->schema());
        $spy = new SpyHydrator();

        static::assertSame(
            Format::rowFrames($encoder->encode((new PhpRowHydrator())->dehydrate($data))),
            $encoder->encodeFrames($data, $spy),
        );
        static::assertSame(1, $spy->dehydrateCalls);
    }

    public function test_encode_frames_turns_a_row_without_a_declared_column_into_a_floe_exception(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('flow_php found a row that does not carry the declared column "name"');

        (new NativeFloeEncoder($schema))->encodeFrames(Rows::trusted($schema, [row([
            'id' => 1,
        ])]), new NativeRowHydrator());
    }
}
