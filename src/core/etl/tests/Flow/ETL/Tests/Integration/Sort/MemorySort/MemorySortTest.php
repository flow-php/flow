<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\MemorySort;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Pipeline;
use Flow\ETL\Sort\MemorySort;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class MemorySortTest extends FlowTestCase
{
    public function test_memory_implementation_of_external_sort_algorithm(): void
    {
        $input = [];

        for ($j = 10; $j > 0; $j--) {
            for ($i = 10; $i > 0; $i--) {
                $input[] = [
                    'id' =>
                        str_pad((string) $j, 5, '0', STR_PAD_LEFT) . '-' . str_pad((string) $i, 3, '0', STR_PAD_LEFT),
                ];
            }
        }

        $randomizedInput = $input;
        \shuffle($randomizedInput);

        $sort = new MemorySort(Unit::fromMb(1024));

        $context = flow_context();
        $pipeline = new Pipeline(from_array($randomizedInput));

        $sortedOutput = \iterator_to_array($sort->sortGenerator(
            $pipeline->process($context),
            $context,
            refs(ref('id')->desc()),
        ));

        static::assertEquals($input, \array_merge(...\array_map(static fn($row) => $row->toArray(), $sortedOutput)));
    }
}
