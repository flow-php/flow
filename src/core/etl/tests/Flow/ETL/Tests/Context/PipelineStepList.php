<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Planning\Physical\TransactionGroup;

use function array_map;

final class PipelineStepList
{
    /**
     * $pipeline's steps in execution order, its breaker last.
     *
     * @return list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer>
     */
    public static function of(Pipeline $pipeline): array
    {
        $breaker = $pipeline->steps->breaker;

        return $breaker === null ? $pipeline->steps->steps : [...$pipeline->steps->steps, $breaker];
    }

    /**
     * @return list<class-string> a join step listed by its join
     */
    public static function classes(Pipeline $pipeline): array
    {
        return array_map(
            static fn(JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer $step): string => $step
                instanceof JoinStep
                    ? $step->join::class
                    : $step::class,
            self::of($pipeline),
        );
    }
}
