<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Operators\Processor;

use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Tests\Context\ExecutedPipelines;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function iterator_to_array;
use function shuffle;

final class MemorySortProcessorTest extends FlowTestCase
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
        shuffle($randomizedInput);

        $processor = new MemorySortProcessor([ref('id')->desc()]);

        $context = flow_context();
        $source = from_array($randomizedInput);
        $steps = [];

        static::assertEquals(
            $input,
            array_merge(...array_map(
                static fn($row) => $row->toArray(),
                iterator_to_array($processor->process(
                    ExecutedPipelines::of($source, $steps, $context),
                    $context->backend(),
                )),
            )),
        );
    }
}
