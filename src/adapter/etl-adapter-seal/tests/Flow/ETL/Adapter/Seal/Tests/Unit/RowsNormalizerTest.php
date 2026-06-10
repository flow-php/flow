<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use Flow\ETL\Adapter\Seal\RowsNormalizer;
use Flow\ETL\Adapter\Seal\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function iterator_to_array;

final class RowsNormalizerTest extends FlowTestCase
{
    public function test_normalizing_empty_rows_yields_no_documents(): void
    {
        $normalizer = new RowsNormalizer(new EntryNormalizer());

        static::assertSame([], iterator_to_array($normalizer->normalize(rows())));
    }

    public function test_normalizing_rows_into_documents_keyed_by_entry_names(): void
    {
        $normalizer = new RowsNormalizer(new EntryNormalizer());

        static::assertSame(
            [
                ['id' => '1', 'age' => 30, 'active' => true],
                ['id' => '2', 'age' => 25, 'active' => false],
            ],
            iterator_to_array($normalizer->normalize(rows(
                row(string_entry('id', '1'), integer_entry('age', 30), bool_entry('active', true)),
                row(string_entry('id', '2'), integer_entry('age', 25), bool_entry('active', false)),
            ))),
        );
    }
}
