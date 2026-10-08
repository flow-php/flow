<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Generator;

final readonly class ExpandingProcessor implements Processor
{
    /**
     * @var int<1, max>
     */
    private int $batchSize;

    /**
     * @param Definition<mixed>|string $entry
     * @param int $batchSize the most rows one output batch holds
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private string|Definition $entry,
        public ScalarFunction $function,
        private RequiredColumns $carries,
        int $batchSize = 1000,
        private ?BoundExpansion $bound = null,
    ) {
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $batchSize);
        }

        $this->batchSize = $batchSize;
    }

    public function bind(Schema $input): BoundStep
    {
        $bound = BoundExpansion::of($this->entry, $this->function, $this->carries, $input);

        return new BoundStep(
            new self($this->entry, $this->function, $this->carries, $this->batchSize, $bound),
            $bound->output,
        );
    }

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        while ($rows->valid()) {
            /** @var Rows $batch */
            $batch = $rows->current();
            // a plan that refused a schema runs this step unbound
            $bound = $this->bound ?? BoundExpansion::of(
                $this->entry,
                $this->function,
                $this->carries,
                $batch->schema(),
            );

            foreach ($bound->chunks($batch, $context, $this->batchSize) as $chunk) {
                $signal = yield $chunk;

                if ($signal === Signal::STOP) {
                    $rows->send(Signal::STOP);

                    return;
                }
            }

            $rows->next();
        }
    }
}
