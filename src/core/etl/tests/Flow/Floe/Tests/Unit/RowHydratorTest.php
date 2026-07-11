<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\EntryInstantiator;
use Flow\Floe\FloeWriter;
use Flow\Floe\RowEncoder;
use Flow\Floe\RowHydrator;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\ValueDecoder;
use Flow\Serializer\Exception\SerializationException;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function serialize;
use function strlen;

final class RowHydratorTest extends TestCase
{
    public function test_hydrated_row_equals_original_row(): void
    {
        $original = row(int_entry('id', 42), str_entry('name', 'flow'), str_entry('nullable', null));

        $encoderPlan = FloeWriter::growSectionPlan(null, $original);
        $body = (new RowEncoder())->encode($encoderPlan, $original);

        $hydratorPlan = (new SchemaDecoder(
            new ValueDecoder(),
            new EntryInstantiator(),
        ))->decode($encoderPlan->schemaBody);
        $position = 0;
        $hydrated = (new RowHydrator())->hydrate($hydratorPlan, $body, $position);

        static::assertEquals($original, $hydrated);
        static::assertSame(strlen($body), $position);
        static::assertSame(serialize($original), serialize($hydrated));
    }

    public function test_hydrated_definitions_are_not_shared_between_rows(): void
    {
        $original = row(int_entry('id', 1));

        $encoderPlan = FloeWriter::growSectionPlan(null, $original);
        $body = (new RowEncoder())->encode($encoderPlan, $original);

        $hydratorPlan = (new SchemaDecoder(
            new ValueDecoder(),
            new EntryInstantiator(),
        ))->decode($encoderPlan->schemaBody);
        $hydrator = new RowHydrator();

        $position = 0;
        $first = $hydrator->hydrate($hydratorPlan, $body, $position);
        $position = 0;
        $second = $hydrator->hydrate($hydratorPlan, $body, $position);

        static::assertNotSame($first->entries()['id']->definition(), $second->entries()['id']->definition());
    }

    public function test_hydrating_row_with_unknown_value_flag_throws(): void
    {
        $original = row(int_entry('id', 1));
        $schemaJson = FloeWriter::growSectionPlan(null, $original)->schemaBody;
        $hydratorPlan = (new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()))->decode($schemaJson);
        $position = 0;

        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('unknown value flag');

        (new RowHydrator())->hydrate($hydratorPlan, "\xEF", $position);
    }
}
