<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SchemaDecoderTest extends TestCase
{
    public function test_decoded_plan_provides_the_base_definition(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(schema(str_schema('name')));

        $plan = (new SchemaDecoder(new ValueDecoder()))->decode($schemaJson);

        static::assertCount(1, $plan);
        static::assertSame('name', $plan[0]->name);
        static::assertFalse($plan[0]->definition->isNullable());
    }

    public function test_decoded_plan_maps_a_null_column_to_a_null_definition(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(schema(null_schema('name')));

        $plan = (new SchemaDecoder(new ValueDecoder()))->decode($schemaJson);

        static::assertInstanceOf(NullDefinition::class, $plan[0]->definition);
        static::assertTrue($plan[0]->definition->isNullable());
    }

    public function test_decoded_plan_preserves_custom_metadata(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(schema(int_schema('id', metadata: Metadata::fromArray([
            'custom' => 'meta',
        ]))));

        $plan = (new SchemaDecoder(new ValueDecoder()))->decode($schemaJson);

        static::assertSame('meta', $plan[0]->definition->metadata()->get('custom'));
    }

    public function test_decoding_invalid_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to decode schema JSON');

        (new SchemaDecoder(new ValueDecoder()))->decode('{invalid');
    }

    public function test_decoding_non_list_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('expected schema JSON to be a list');

        (new SchemaDecoder(new ValueDecoder()))->decode('"scalar"');
    }
}
