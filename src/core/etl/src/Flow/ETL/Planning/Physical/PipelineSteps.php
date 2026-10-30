<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;

use function array_search;
use function array_slice;
use function array_values;

final readonly class PipelineSteps
{
    /**
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps in execution order; a
     *                                                                                         processor or cross join
     *                                                                                         among them streams
     * @param null|JoinStep|Processor $breaker the step that needs every row before it emits, ending the pipeline
     */
    public function __construct(
        public array $steps = [],
        public JoinStep|Processor|null $breaker = null,
    ) {}

    /**
     * What remains from $step on: the steps from it and the breaker. Nothing remains when $step is null.
     */
    public function from(JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer|null $step): self
    {
        if ($step === null) {
            return new self();
        }

        $at = array_search($step, $this->steps, true);

        return new self($at === false ? [] : array_values(array_slice($this->steps, $at)), $this->breaker);
    }

    public function isEmpty(): bool
    {
        return $this->steps === [] && $this->breaker === null;
    }

    /**
     * A step after $step, in this pipeline, that is an instance of one of $classes.
     *
     * @param class-string ...$classes
     */
    public function followedBy(object $step, string ...$classes): bool
    {
        $after = false;

        foreach ($this->steps as $candidate) {
            if ($after) {
                foreach ($classes as $class) {
                    if ($candidate instanceof $class) {
                        return true;
                    }
                }
            }

            $after = $after || $candidate === $step;
        }

        return false;
    }
}
