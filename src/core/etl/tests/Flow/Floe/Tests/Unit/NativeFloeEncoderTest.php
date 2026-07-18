<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_entry;
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
        $schema = schema_from_json(
            FloeSchemaContext::schemaBody(row(int_entry('id', 1), str_entry('name', 'flow'))->schema()),
        );

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
            row(int_entry('id', 1), str_entry('name', 'flow')),
            row(int_entry('id', 2), str_entry('name', null)),
        );

        $schema = schema_from_json(FloeSchemaContext::schemaBody($data->first()->schema()));
        $bodies = (new PhpFloeEncoder($schema))->encode((new PhpRowHydrator())->dehydrate($data));

        static::assertEquals(
            (new PhpFloeEncoder($schema))->decode($bodies),
            (new NativeFloeEncoder($schema))->decode($bodies),
        );
    }

    public function test_native_metadata_bearing_frames_match_the_php_engine(): void
    {
        $schema = schema_from_json(
            FloeSchemaContext::schemaBody(row(int_entry('id', 1), str_entry('name', 'x'))->schema()),
        );

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
}
