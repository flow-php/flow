<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Sink;

/**
 * Writes the frame it is given into every sink, so the sinks share that frame's nodes.
 */
final readonly class FanOutSink implements Sink
{
    /**
     * @var list<Loader|Sink>
     */
    private array $sinks;

    public function __construct(Loader|Sink ...$sinks)
    {
        $this->sinks = array_values($sinks);
    }

    public function write(DataFrame $prefix): void
    {
        foreach ($this->sinks as $sink) {
            $prefix->write($sink);
        }
    }
}
