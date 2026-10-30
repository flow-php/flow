<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Reference;

use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class ReferencesTest extends FlowTestCase
{
    public function test_lazy_without(): void
    {
        static::assertEquals(refs('name')->all(), refs()->without('id')->add('id')->add('name')->all());
    }

    public function test_references_names(): void
    {
        static::assertEquals(['id', 'name'], refs('id', 'name')->names());
    }

    public function test_that_reference_with_alias_exists(): void
    {
        $refs = new References(ref('id')->as('test'), ref('name'));

        static::assertFalse($refs->has(ref('id')));
        static::assertTrue($refs->has(ref('test')));
    }

    public function test_that_reference_without_alias_exists(): void
    {
        $refs = new References(ref('id'), ref('name'));

        static::assertTrue($refs->has(ref('id')));
        static::assertFalse($refs->has(ref('test')));
    }

    public function test_without(): void
    {
        static::assertEquals(refs('name')->all(), refs('id', 'name')->without('id')->all());
    }

    public function test_without_survives_the_round_trip(): void
    {
        $rebuilt = DescribeRoundTrip::rebuilt(refs('a', 'b')->without('b'));

        static::assertSame(['a'], $rebuilt->names());
        static::assertSame(['a'], $rebuilt->add('b')->names());
    }
}
