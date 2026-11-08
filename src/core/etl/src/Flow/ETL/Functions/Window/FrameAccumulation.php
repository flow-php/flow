<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Foundation\Progress;
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
    public function values(FrameAccumulating $function, Rows $partition, Backend $backend, Progress $progress): array
    {
        $bounds = [];
        $unreported = 0;

        for ($index = 0, $count = $partition->count(); $index < $count; $index++) {
            $bounds[] = $this->frame->bounds($index, $partition);

            if (++$unreported === Progress::ROWS_PER_CALL) {
                $progress->advanced($unreported);
                $unreported = 0;
            }
        }

        $progress->advanced($unreported);

        return $function->frameValues($partition, $bounds, $backend, $progress);
    }
}
