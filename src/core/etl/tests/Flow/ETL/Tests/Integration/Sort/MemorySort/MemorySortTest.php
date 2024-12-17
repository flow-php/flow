<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\MemorySort;

use function Flow\ETL\DSL\{flow_context, from_array, ref, refs};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Sort\MemorySort;

final class MemorySortTest extends TestCase
{
    public function test_memory_implementation_of_external_sort_algorithm() : void
    {
        $input = [];

        for ($j = 10; $j > 0; $j--) {
            for ($i = 10; $i > 0; $i--) {
                $input[] = ['id' => str_pad((string) $j, 5, '0', STR_PAD_LEFT) . '-' . str_pad((string) $i, 3, '0', STR_PAD_LEFT)];
            }
        }

        $randomizedInput = $input;
        \shuffle($randomizedInput);

        $sort = new MemorySort(
            Unit::fromMb(1024)
        );

        $sortedOutput = \iterator_to_array(
            $sort->sortBy(
                new SynchronousPipeline(from_array($randomizedInput)),
                flow_context(),
                refs(ref('id')->desc())
            )->extract(flow_context())
        );

        self::assertEquals(
            $input,
            \array_merge(...\array_map(
                fn ($row) => $row->toArray(),
                $sortedOutput
            ))
        );
    }
}
