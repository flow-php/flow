<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_rows, json_entry, ref};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Transformer\{RenameStrReplaceAllEntriesTransformer};
use PHPUnit\Framework\Attributes\{IgnoreDeprecations};

#[IgnoreDeprecations]
final class RenameStrReplaceAllCaseTransformerTest extends FlowIntegrationTestCase
{
    public function test_rename_all() : void
    {
        $rows = rows(row(json_entry('array', ['id' => 1, 'name' => 'name', 'active' => true])), row(json_entry('array', ['id' => 2, 'name' => 'name', 'active' => false])));

        $ds = df()
            ->read(from_rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->transform(new RenameStrReplaceAllEntriesTransformer('row.', ''))
            ->drop('array')
            ->getEachAsArray();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }
}
