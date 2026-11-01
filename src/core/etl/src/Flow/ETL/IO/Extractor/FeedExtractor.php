<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Fiber;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Must be driven inside a Fiber: extract() suspends when starved, finish() ends the stream.
 */
final class FeedExtractor implements Extractor
{
    private ?Rows $batch = null;

    private bool $finished = false;

    public function __construct(
        private Schema $schema,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        while (true) {
            if ($this->batch === null) {
                if ($this->finished) {
                    return;
                }

                Fiber::suspend();

                continue;
            }

            $rows = $this->batch;
            $this->batch = null;

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            // @mago-ignore analysis:redundant-comparison,redundant-logical-operation
            // Mago does not model the generator suspension above: feed() and finish() run while the enclosing Fiber
            // is parked, so both operands can change between the yield and this check.
            if ($this->batch === null && !$this->finished) {
                // a downstream projection still sees the columns it was built against
                $signal = yield Rows::empty($this->schema, $backend);

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }
    }

    public function feed(Rows $rows): void
    {
        $this->batch = $rows;
    }

    public function finish(): void
    {
        $this->finished = true;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
