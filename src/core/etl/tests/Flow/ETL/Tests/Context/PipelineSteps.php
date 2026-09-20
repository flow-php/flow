<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Executor\Segments;
use Flow\ETL\Loader;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

use function array_map;

final class PipelineSteps
{
    /**
     * @return list<Loader|Processor|Transformer>
     */
    public static function of(Segments $segments): array
    {
        $steps = [];

        foreach ($segments->all() as $segment) {
            foreach ($segment->steps() as $step) {
                $steps[] = $step;
            }

            $processor = $segment->processor();

            if ($processor !== null) {
                $steps[] = $processor;
            }
        }

        return $steps;
    }

    /**
     * @return list<class-string>
     */
    public static function classes(Segments $segments): array
    {
        return array_map(static fn(Loader|Processor|Transformer $step): string => $step::class, self::of($segments));
    }
}
