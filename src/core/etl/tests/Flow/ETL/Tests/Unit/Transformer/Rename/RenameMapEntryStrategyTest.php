<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\Rename;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\rename_map;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class RenameMapEntryStrategyTest extends FlowTestCase
{
    public function test_rename_filters_only_existing_names(): void
    {
        $strategy = rename_map([
            'old_a' => 'new_a',
            'old_b' => 'new_b',
            'non_existent' => 'ignored',
        ]);

        $result = $strategy->rename(row(
            str_entry('old_a', 'value_a'),
            str_entry('old_b', 'value_b'),
            str_entry('other_column', 'value_c'),
        ));

        $entryNames = \array_keys($result->entries()->toArray());
        \sort($entryNames);
        static::assertSame(['new_a', 'new_b', 'other_column'], $entryNames);
    }

    public function test_rename_returns_unchanged_row_when_no_matching_names(): void
    {
        $strategy = rename_map(['old_name' => 'new_name']);
        $row = row(str_entry('different_column', 'value'));

        $result = $strategy->rename($row);

        static::assertSame($row, $result);
    }

    public function test_rename_with_empty_renames(): void
    {
        $strategy = rename_map([]);
        $row = row(str_entry('column_a', 'a'), int_entry('column_b', 1));

        $result = $strategy->rename($row);

        static::assertSame($row, $result);
    }

    public function test_rename_with_empty_row(): void
    {
        $strategy = rename_map(['old_name' => 'new_name']);
        $row = row();

        $result = $strategy->rename($row);

        static::assertSame($row, $result);
    }
}
