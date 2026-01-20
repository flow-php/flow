<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\Transformer;

use function Flow\ETL\DSL\{array_to_rows, config, flow_context, rename_style};
use Flow\ETL\{FlowContext, Rows, String\StringStyles, Transformer\RenameEachEntryTransformer};
use PhpBench\Attributes\{BeforeMethods, Groups};

#[BeforeMethods('setUp')]
#[Groups(['data-frame'])]
final class RenameEachEntryTransformerBench
{
    private FlowContext $context;

    private Rows $rows;

    public function setUp() : void
    {
        $this->rows = array_to_rows(
            \array_merge(...\array_map(static fn () : array => [
                ['id' => 1, 'random text' => null, 'from' => 666],
                ['id' => 2, 'random text' => null, 'from' => 666],
                ['id' => 3, 'random text' => null, 'from' => 666],
                ['id' => 4, 'random text' => null, 'from' => 666],
                ['id' => 5, 'random text' => null, 'from' => 666],
            ], \range(0, 199))),
            flow_context(config())->entryFactory(),
        );
        $this->context = flow_context(config());
    }

    public function bench_transform_1k_rows() : void
    {
        (new RenameEachEntryTransformer(rename_style(StringStyles::KEBAB)))->transform($this->rows, $this->context);
    }
}
