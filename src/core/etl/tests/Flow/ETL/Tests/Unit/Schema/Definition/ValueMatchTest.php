<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\ValueMatch;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ValueMatchTest extends FlowTestCase
{
    public function test_a_non_nullable_definition_rejects_null(): void
    {
        static::assertFalse((new ValueMatch())->matches(int_schema('id'), null));
    }

    public function test_a_null_definition_accepts_null(): void
    {
        static::assertTrue((new ValueMatch())->matches(null_schema('id'), null));
    }

    public function test_a_null_definition_rejects_a_non_null_value(): void
    {
        static::assertFalse((new ValueMatch())->matches(null_schema('id'), 1));
    }

    public function test_a_nullable_definition_accepts_null(): void
    {
        static::assertTrue((new ValueMatch())->matches(int_schema('id', nullable: true), null));
    }

    public function test_a_value_of_a_different_type_is_rejected(): void
    {
        static::assertFalse((new ValueMatch())->matches(int_schema('id'), 'one'));
    }

    public function test_a_value_of_the_declared_type_is_accepted(): void
    {
        static::assertTrue((new ValueMatch())->matches(int_schema('id'), 1));
    }

    public function test_nullability_does_not_widen_the_declared_type(): void
    {
        static::assertFalse((new ValueMatch())->matches(str_schema('name', nullable: true), 1));
    }

    public function test_the_declared_type_validates_the_value_it_describes(): void
    {
        static::assertTrue((new ValueMatch())->matches(list_schema('tags', type_list(type_string())), ['a', 'b']));
        static::assertFalse((new ValueMatch())->matches(list_schema('tags', type_list(type_string())), [1, 2]));
    }
}
