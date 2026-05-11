<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\References;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class ReferencesTest extends FlowTestCase
{
    public function test_lazy_without(): void
    {
        $refs = refs()->without('id')->add('id')->add('name');

        static::assertEquals(refs('name')->all(), $refs->all());
    }

    public function test_references_names(): void
    {
        $refs = refs('id', 'name');

        static::assertEquals(['id', 'name'], $refs->names());
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
}
