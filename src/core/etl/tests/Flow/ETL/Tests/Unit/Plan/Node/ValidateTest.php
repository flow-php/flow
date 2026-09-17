<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Validate;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ValidateTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Validate($input, schema(int_schema('id')), new StrictValidator());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $schema = schema(int_schema('id'));
        $validator = new StrictValidator();
        $node = new Validate($input, $schema, $validator);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Validate::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($schema, $rebuilt->schema);
        static::assertSame($validator, $rebuilt->validator);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Validate($input, schema(int_schema('id')), new StrictValidator());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
