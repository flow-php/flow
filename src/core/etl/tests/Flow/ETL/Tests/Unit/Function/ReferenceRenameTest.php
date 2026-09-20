<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ReferenceRename;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class ReferenceRenameTest extends FlowTestCase
{
    public function test_a_reference_to_the_renamed_column_points_at_the_new_one(): void
    {
        static::assertEquals(ref('year'), (new ReferenceRename('y', 'year'))->in(ref('y')));
    }

    public function test_every_reference_inside_the_tree_is_renamed_and_the_rest_kept(): void
    {
        static::assertEquals(
            ref('year')->equals(lit(2023))->and(ref('month')->equals(lit('07'))),
            (new ReferenceRename('y', 'year'))->in(ref('y')->equals(lit(2023))->and(ref('month')->equals(lit('07')))),
        );
    }

    public function test_a_tree_without_the_column_is_returned_as_is(): void
    {
        $tree = ref('month')->equals(lit('07'));

        static::assertSame($tree, (new ReferenceRename('y', 'year'))->in($tree));
    }

    public function test_an_alias_on_the_reference_is_kept(): void
    {
        static::assertEquals(ref('year')->as('label'), (new ReferenceRename('y', 'year'))->in(ref('y')->as('label')));
    }

    public function test_a_resolved_reference_to_the_column_cannot_be_renamed(): void
    {
        static::assertNull((new ReferenceRename('y', 'year'))->in(ref('y')->resolve(int_schema('y'))));
    }
}
