<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Functions\FrameAccumulating;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

final readonly class FrameAccumulation
{
    public function __construct(
        private WindowFrame $frame,
    ) {}

    /**
     * One value per row of the sorted $partition.
     *
     * @return list<mixed>
     */
    public function values(FrameAccumulating $function, Rows $partition, Backend $backend): array
    {
        $bounds = [];

        for ($index = 0, $count = $partition->count(); $index < $count; $index++) {
            $bounds[] = $this->frame->bounds($index, $partition);
        }

        return $function->frameValues($partition, $bounds, $backend);
    }
}
