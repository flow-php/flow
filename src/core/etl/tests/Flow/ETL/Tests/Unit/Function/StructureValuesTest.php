<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\StructureValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\StructureType;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

final class StructureValuesTest extends FlowTestCase
{
    public function test_unifies_required_and_optional_field_types(): void
    {
        static::assertSame(
            'float',
            StructureValues::type('fn', new StructureType(['a' => type_integer()], ['b' => type_float()]))->toString(),
        );
    }

    public function test_a_nullable_field_makes_the_unified_type_nullable(): void
    {
        static::assertSame(
            '?float',
            StructureValues::type('fn', new StructureType([
                'a' => type_integer(),
                'b' => type_optional(type_float()),
            ]))->toString(),
        );
    }

    public function test_refuses_fields_that_do_not_unify(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('whose field types do not unify');

        StructureValues::type('fn', new StructureType(['a' => type_boolean(), 'b' => type_list(type_integer())]));
    }
}
