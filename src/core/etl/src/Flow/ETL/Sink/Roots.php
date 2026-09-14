<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Sink;

/**
 * @internal the only Loader|Sink switch in the codebase
 *
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
final readonly class Roots
{
    /**
     * Every verb mutates the frame it is called on, so each call gets its OWN fork: one frame handed to two
     * children would alias them. The prefix's own write()s are read AFTER roots() ran.
     *
     * @return Sinks
     */
    public function of(DataFrame $from, Loader|Sink $sink): array
    {
        $prefix = $from->fork();
        $roots = $sink instanceof Sink ? $sink->roots($prefix) : [new Node\Write($prefix->cursor(), $sink)];

        return [...$roots, ...$prefix->sinks()];
    }
}
