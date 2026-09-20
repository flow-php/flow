<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class ReferencedColumnsTest extends FlowTestCase
{
    public function test_a_bare_reference_is_its_own_source_column(): void
    {
        static::assertSame(['a'], (new ReferencedColumns())->in(ref('a'))->names());
    }

    public function test_an_alias_reports_the_source_column(): void
    {
        static::assertSame(['year'], (new ReferencedColumns())->in(ref('year')->as('y'))->names());
    }

    public function test_a_nested_tree_reports_every_column_once(): void
    {
        static::assertSame(
            ['a', 'b'],
            (new ReferencedColumns())->in(ref('a')->equals(ref('b'))->and(ref('a')->isNotNull()))->names(),
        );
    }

    public function test_a_literal_only_predicate_references_nothing(): void
    {
        static::assertSame([], (new ReferencedColumns())->in(lit(true))->names());
    }
}
