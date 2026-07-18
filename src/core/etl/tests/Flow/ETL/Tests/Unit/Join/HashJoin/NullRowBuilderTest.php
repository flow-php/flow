<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class NullRowBuilderTest extends FlowTestCase
{
    public function test_collects_every_entry_name_across_rows(): void
    {
        $builder = new NullRowBuilder(new EntryFactory());

        $builder->collect(row(int_entry('id', 1), str_entry('name', 'Alice')));
        $builder->collect(row(int_entry('id', 2), str_entry('country', 'PL')));

        static::assertSame(['id' => null, 'name' => null, 'country' => null], $builder->row()->toArray());
    }

    public function test_definitions_are_nullable(): void
    {
        $builder = new NullRowBuilder(new EntryFactory());

        $builder->collect(row(int_entry('id', 1)));

        static::assertTrue($builder->row()->get('id')->definition()->isNullable());
    }

    public function test_empty_builder_produces_empty_row(): void
    {
        static::assertSame([], (new NullRowBuilder(new EntryFactory()))->row()->toArray());
    }

    public function test_first_seen_definition_wins(): void
    {
        $builder = new NullRowBuilder(new EntryFactory());

        $builder->collect(row(int_entry('id', 1)));
        $builder->collect(row(str_entry('id', 'one')));

        static::assertSame('integer', $builder->row()->get('id')->definition()->type()->toString());
    }
}
