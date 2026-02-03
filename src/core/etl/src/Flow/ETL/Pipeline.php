<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Segments;

/**
 * @internal
 */
final readonly class Pipeline
{
    private Segments $stages;

    public function __construct(private Extractor $extractor)
    {
        $this->stages = new Segments();
    }

    public function add(Transformer|Loader|Processor $step) : self
    {
        $this->stages->add($step);

        return $this;
    }

    /**
     * Get the pipeline extractor.
     */
    public function extractor() : Extractor
    {
        return $this->extractor;
    }

    /**
     * Check if pipeline contains a step of the given class.
     *
     * @param class-string<Loader|Processor|Transformer> $class
     */
    public function has(string $class) : bool
    {
        return $this->stages->has($class);
    }

    /**
     * Process the pipeline and yield Rows batches.
     *
     * @return \Generator<int, Rows>
     */
    public function process(FlowContext $context) : \Generator
    {
        $generator = $this->extractor->extract($context);

        foreach ($this->stages->all() as $segment) {
            $generator = $segment->execute($generator, $context);

            if ($segment->processor() !== null) {
                $generator = $segment->processor()->process($generator, $context);
            }
        }

        foreach ($generator as $rows) {
            yield $rows;
        }
    }

    /**
     * Get the pipeline stages.
     */
    public function stages() : Segments
    {
        return $this->stages;
    }
}
