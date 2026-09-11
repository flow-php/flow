<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\Datasets\Datasets;

/**
 * One-column sources that cannot carry the orders schema, so they run their own pipeline family.
 */
enum NodeSource: string
{
    case text = 'text';
    case xml = 'xml';

    public function path(int $rows): string
    {
        return match ($this) {
            self::text => Datasets::text($rows)->path(),
            self::xml => Datasets::orders($rows)->xml(),
        };
    }
}
