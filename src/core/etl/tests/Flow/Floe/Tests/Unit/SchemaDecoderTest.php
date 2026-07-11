<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\EntryInstantiator;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeWriter;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SchemaDecoderTest extends TestCase
{
    public function test_decoded_plan_provides_nullable_and_from_null_definition_variants(): void
    {
        $schemaJson = FloeWriter::growSectionPlan(null, row(str_entry('name', 'x')))->schemaBody;

        $plan = (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode($schemaJson);

        static::assertCount(1, $plan);
        static::assertSame('name', $plan[0]->name);
        static::assertFalse($plan[0]->definition->isNullable());
        static::assertTrue($plan[0]->nullableDefinition->isNullable());
        static::assertTrue($plan[0]->fromNullDefinition->isNullable());
        static::assertTrue($plan[0]->fromNullDefinition->metadata()->has(Metadata::FROM_NULL));
        static::assertFalse($plan[0]->nullableDefinition->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_decoded_plan_strips_from_null_marker_from_base_definition(): void
    {
        $schemaJson = FloeWriter::growSectionPlan(null, row(StringEntry::fromNull('name')))->schemaBody;

        $plan = (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode($schemaJson);

        static::assertFalse($plan[0]->definition->metadata()->has(Metadata::FROM_NULL));
        static::assertTrue($plan[0]->fromNullDefinition->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_decoded_plan_preserves_custom_metadata(): void
    {
        $schemaJson = FloeWriter::growSectionPlan(null, row(int_entry('id', 1, Metadata::fromArray([
            'custom' => 'meta',
        ]))))->schemaBody;

        $plan = (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode($schemaJson);

        static::assertSame('meta', $plan[0]->definition->metadata()->get('custom'));
    }

    public function test_decoding_invalid_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to decode schema JSON');

        (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode('{invalid');
    }

    public function test_decoding_non_list_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('expected schema JSON to be a list');

        (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode('"scalar"');
    }
}
