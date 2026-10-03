<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Layout\BufferLayout;
use Flow\ETL\Exception\OffsetOverflow;
use Flow\ETL\Rows;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FrameTooLarge;

use function pack;
use function sprintf;
use function str_repeat;
use function strlen;

final readonly class FrameEncoder
{
    public function __construct(
        private Codec $codec = new NoopCodec(),
        private BufferLayout $layout = new BufferLayout(),
    ) {}

    /**
     * @throws OffsetOverflow
     * @throws FrameTooLarge
     */
    public function encode(Rows $rows): string
    {
        $nodes = '';
        $extents = '';
        $area = '';
        $nodeCount = 0;
        $bufferCount = 0;

        foreach ($rows->schema()->definitions() as $definition) {
            $column = $rows->column($definition->entry()->name());
            $buffers = $column->encode();

            $columnNodes = $this->layout->nodeCount($definition->type()) === 1
                ? [[$rows->count(), $column->nullCount()]]
                : $this->layout->nodes($definition->type(), $rows->count(), $column->nullCount(), $buffers);

            foreach ($columnNodes as [$length, $nulls]) {
                $nodes .= pack('VV', $length, $nulls);
                $nodeCount++;
            }

            foreach ($buffers as $buffer) {
                $bufferCount++;

                if ($buffer === '') {
                    $extents .= pack('VV', 0, 0);

                    continue;
                }

                $encoded = $this->codec->encode($buffer);
                $stored = strlen($encoded) < strlen($buffer)
                    ? pack('P', strlen($buffer)) . $encoded
                    : pack('P', -1) . $buffer;
                $extents .= pack('VV', strlen($area), strlen($stored));
                $area .= $stored . str_repeat("\0", (8 - (strlen($stored) % 8)) % 8);
            }
        }

        $directory = pack('VVV', $rows->count(), $nodeCount, $bufferCount) . $nodes . $extents;
        $body = $directory . str_repeat("\0", (8 - (strlen($directory) % 8)) % 8) . $area;

        if (strlen($body) > 0xFFFFFFFF) {
            throw new FrameTooLarge(sprintf(
                'Floe BATCH frame body of %d bytes exceeds the 4 GiB frame length',
                strlen($body),
            ));
        }

        return $body;
    }
}
