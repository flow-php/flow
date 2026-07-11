<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Rows;
use Flow\Floe\Exception\FloeException;

interface RowFrameEncoder
{
    /**
     * @throws FloeException
     *
     * @return array<int, FrameSegment>
     */
    public function encode(Rows $rows): array;
}
