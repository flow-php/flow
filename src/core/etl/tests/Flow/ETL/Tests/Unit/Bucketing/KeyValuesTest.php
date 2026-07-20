<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class KeyValuesTest extends FlowTestCase
{
    public function test_of_projects_each_row(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new KeyValues(refs('id')))->of(rows(row(int_entry('id', 1)), row(int_entry('id', 2)))),
        );
    }

    public function test_of_row_preserves_null(): void
    {
        static::assertSame(['id' => null], (new KeyValues(refs('id')))->ofRow(row(int_entry('id', null))));
    }

    public function test_of_row_projects_refs_in_order(): void
    {
        static::assertSame(
            ['id' => 1, 'country' => 'PL'],
            (new KeyValues(refs('id', 'country')))->ofRow(row(int_entry('id', 1), str_entry('country', 'PL'))),
        );
    }
}
