<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Launcher;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Values\Rows;
use Generator;

final readonly class Launch
{
    /**
     * @param int<1, max> $consumed how many pipelines of the chain the launch runs
     * @param PipelineSteps $rest what the parent runs in the last consumed pipeline, in place of its steps
     * @param Generator<int, Rows, null|Signal, void> $rows the rows after the launched steps, in split order
     */
    public function __construct(
        public int $consumed,
        public PipelineSteps $rest,
        public Generator $rows,
    ) {}
}
