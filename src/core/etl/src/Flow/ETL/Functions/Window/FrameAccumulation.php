<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Functions\FrameAccumulating;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

use function range;

final readonly class FrameAccumulation
{
    public function __construct(
        private WindowFrame $frame,
    ) {}

    /**
     * One value per row of the sorted $partition, reusing the previous value whenever the frame bounds did not move.
     *
     * @return list<mixed>
     */
    public function values(FrameAccumulating $function, Rows $partition, Backend $backend): array
    {
        $values = [];
        $accumulator = null;
        $previousBounds = null;
        $value = null;

        for ($index = 0, $count = $partition->count(); $index < $count; $index++) {
            $bounds = $this->frame->bounds($index, $partition);

            if ($bounds === $previousBounds) {
                $values[] = $value;

                continue;
            }

            // Equal starts with a grown end mean the previous frame is a prefix of this one, so the
            // rows already accumulated still belong to it and only the new tail has to be fed.
            if (
                $accumulator !== null
                && $previousBounds !== null
                && $bounds[0] === $previousBounds[0]
                && $bounds[1] > $previousBounds[1]
            ) {
                $accumulator->aggregate(
                    $partition,
                    $bounds[1] === ($previousBounds[1] + 1) ? [$bounds[1]] : range($previousBounds[1] + 1, $bounds[1]),
                    $backend,
                );
            } else {
                $accumulator = $function->accumulator();

                // an empty frame is [1, 0], and range() would count down over it
                if ($bounds[0] <= $bounds[1]) {
                    $accumulator->aggregate($partition, range($bounds[0], $bounds[1]), $backend);
                }
            }

            $value = $accumulator->value();
            $previousBounds = $bounds;
            $values[] = $value;
        }

        return $values;
    }
}
