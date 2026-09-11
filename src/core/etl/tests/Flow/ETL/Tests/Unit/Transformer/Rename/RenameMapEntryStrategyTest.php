<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\Rename;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\rename_map;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RenameMapEntryStrategyTest extends FlowTestCase
{
    public function test_renames_filters_only_existing_names(): void
    {
        $strategy = rename_map([
            'old_a' => 'new_a',
            'old_b' => 'new_b',
            'non_existent' => 'ignored',
        ]);

        static::assertSame(
            ['old_a' => 'new_a', 'old_b' => 'new_b'],
            $strategy->renames(schema(str_schema('old_a'), str_schema('old_b'), str_schema('other_column'))),
        );
    }

    public function test_renames_returns_nothing_when_no_matching_names(): void
    {
        static::assertSame([], rename_map(['old_name' => 'new_name'])->renames(schema(str_schema('different_column'))));
    }

    public function test_renames_with_empty_renames(): void
    {
        static::assertSame([], rename_map([])->renames(schema(str_schema('column_a'), str_schema('column_b'))));
    }

    public function test_renames_with_empty_schema(): void
    {
        static::assertSame([], rename_map(['old_name' => 'new_name'])->renames(schema()));
    }
}
