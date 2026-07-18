<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SchemaDecoderTest extends TestCase
{
    public function test_decoded_plan_provides_the_base_definition(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(row(str_entry('name', 'x'))->schema());

        $plan = (new SchemaDecoder(new ValueDecoder(), new Instantiators()))->decode($schemaJson);

        static::assertCount(1, $plan);
        static::assertSame('name', $plan[0]->name);
        static::assertFalse($plan[0]->definition->isNullable());
    }

    public function test_decoded_plan_maps_a_null_column_to_a_null_definition(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(row(null_entry('name'))->schema());

        $plan = (new SchemaDecoder(new ValueDecoder(), new Instantiators()))->decode($schemaJson);

        static::assertInstanceOf(NullDefinition::class, $plan[0]->definition);
        static::assertTrue($plan[0]->definition->isNullable());
    }

    public function test_decoded_plan_preserves_custom_metadata(): void
    {
        $schemaJson = FloeSchemaContext::schemaBody(row(int_entry('id', 1, Metadata::fromArray([
            'custom' => 'meta',
        ])))->schema());

        $plan = (new SchemaDecoder(new ValueDecoder(), new Instantiators()))->decode($schemaJson);

        static::assertSame('meta', $plan[0]->definition->metadata()->get('custom'));
    }

    public function test_decoding_invalid_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to decode schema JSON');

        (new SchemaDecoder(new ValueDecoder(), new Instantiators()))->decode('{invalid');
    }

    public function test_decoding_non_list_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('expected schema JSON to be a list');

        (new SchemaDecoder(new ValueDecoder(), new Instantiators()))->decode('"scalar"');
    }
}
