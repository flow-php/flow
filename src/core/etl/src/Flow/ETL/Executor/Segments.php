<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * Manages pipeline segments, grouping steps at Processor boundaries.
 */
final class Segments
{
    private Segment $currentSegment;

    /** @var array<Segment> */
    private array $segments = [];

    public function __construct(?Extractor $extractor = null)
    {
        $this->currentSegment = new Segment(extractor: $extractor);
    }

    public function add(Transformer|Loader|Processor $step): void
    {
        if ($step instanceof Processor) {
            $this->segments[] = $this->currentSegment->withProcessor($step);
            $this->currentSegment = new Segment();
        } else {
            $this->currentSegment->add($step);
        }
    }

    public function extractor(): ?Extractor
    {
        return ($this->segments[0] ?? $this->currentSegment)->extractor();
    }

    /**
     * Get all segments including the current one.
     *
     * @return array<Segment>
     */
    public function all(): array
    {
        return [...$this->segments, $this->currentSegment];
    }
}
