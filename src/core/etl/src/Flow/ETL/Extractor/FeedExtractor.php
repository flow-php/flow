<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Fiber;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Feeds a pipeline from outside, one batch per feed() call. When starved it suspends the enclosing
 * Fiber instead of ending the stream; finish() ends it. The trailing empty Rows after each batch keeps
 * the source one yield ahead of Segment's current()/next() protocol, so a fed batch is processed in the
 * same resume that fed it and a late Signal::STOP still lands on a live yield.
 *
 * Must be driven from inside a Fiber - extract() calls Fiber::suspend().
 *
 * @internal
 */
final class FeedExtractor implements Extractor
{
    private ?Schema $schema = null;

    private ?Rows $batch = null;

    private bool $finished = false;

    private ?Schema $fedSchema = null;

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
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
            $this->fedSchema = $rows->schema();

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            // @mago-ignore analysis:redundant-comparison,redundant-logical-operation
            // Mago does not model the generator suspension above: feed() and finish() run while the enclosing Fiber
            // is parked, so both operands can change between the yield and this check.
            if ($this->batch === null && !$this->finished) {
                // the idle yield keeps the shape of what has been fed, so a downstream projection
                // still sees the columns it was built against
                $signal = yield new Rows($this->schema ?? $this->fedSchema);

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
        if ($this->schema !== null) {
            return $this->schema;
        }

        throw SchemaNotDerivableException::pipeline(self::class);
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
