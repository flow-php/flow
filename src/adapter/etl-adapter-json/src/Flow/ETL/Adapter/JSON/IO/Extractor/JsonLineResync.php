<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\Filesystem\SourceStream;

use function strlen;
use function strpos;

/**
 * A JSON string never holds a raw newline, so the first "\n" at or after a cut is a line boundary - no confirmation
 * pass as for CSV.
 */
final readonly class JsonLineResync
{
    private const int CHUNK = 1 << 12;

    /**
     * The first byte offset at or after $from where a line starts; null when none does. Offset 0 starts one by
     * definition. Monotonic in $from, so neighbouring windows cut at after(offset) share every boundary.
     *
     * @param int<0, max> $from
     *
     * @return null|int<0, max>
     */
    public function after(SourceStream $stream, int $from): ?int
    {
        if ($from === 0) {
            return $stream->read(1, 0) === '' ? null : 0;
        }

        $search = $from - 1;

        while (($chunk = $stream->read(self::CHUNK, $search)) !== '') {
            $found = strpos($chunk, "\n");

            if ($found !== false) {
                return $search + $found + 1;
            }

            $search += strlen($chunk);
        }

        return null;
    }
}
