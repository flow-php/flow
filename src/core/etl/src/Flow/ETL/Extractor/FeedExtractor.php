<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Fiber;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
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
    private ?Rows $batch = null;

    private bool $finished = false;

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

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            // @mago-ignore analysis:redundant-comparison,redundant-logical-operation
            // Mago does not model the generator suspension above: feed() and finish() run while the enclosing Fiber
            // is parked, so both operands can change between the yield and this check.
            if ($this->batch === null && !$this->finished) {
                $signal = yield new Rows();

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
}
