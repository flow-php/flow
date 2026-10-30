<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Flow\ETL\Foundation\Exception\OffsetOverflow;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\FrameTooLarge;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;

use function array_shift;
use function array_unshift;
use function intdiv;
use function sprintf;

final readonly class SplittingFrameEncoder
{
    public function __construct(
        private Backend $backend,
        private Codec $codec = new NoopCodec(),
    ) {}

    /**
     * One frame when the batch fits; otherwise halved until every part does.
     *
     * @throws OffsetOverflow a single row past the frame's offset range, its row named
     * @throws FrameTooLarge a single row past the frame's size, its row named
     *
     * @return list<array{string, int}> [frame body, rows it holds], in row order
     */
    public function encode(Rows $rows): array
    {
        $bodies = [];
        $parts = [[$rows, 0]];

        while ($parts !== []) {
            [$part, $start] = array_shift($parts);

            try {
                $bodies[] = [$this->backend->encodeFrame($part, $this->codec), $part->count()];
            } catch (OffsetOverflow|FrameTooLarge $e) {
                if ($part->count() === 1) {
                    $message = sprintf('%s (row %d)', $e->getMessage(), $start);

                    throw $e instanceof OffsetOverflow
                        ? new OffsetOverflow($message, 0, $e)
                        : new FrameTooLarge($message, 0, $e);
                }

                $half = intdiv($part->count(), 2);
                array_unshift(
                    $parts,
                    [$part->slice(0, $half), $start],
                    [$part->slice($half, $part->count() - $half), $start + $half],
                );
            }
        }

        return $bodies;
    }
}
