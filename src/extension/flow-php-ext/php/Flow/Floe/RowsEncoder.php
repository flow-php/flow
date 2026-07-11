<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Floe\Exception\ExtensionException;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

/**
 * Stateful Floe frame encoder for streaming writes - the PHP side primes the
 * schema and hands over rows; the extension returns bare ROW frame bodies.
 */
final class RowsEncoder
{
    public function __construct()
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @throws ExtensionException
     */
    public function row(Row $row): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * Encodes a whole Rows batch into section segments in one call; segment
     * state persists across calls, independent from the schema()/row() state.
     *
     * @throws ExtensionException
     *
     * @return array<int, FrameSegment>
     */
    public function rows(Rows $rows): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @throws ExtensionException
     */
    public function schema(string $frameBody): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
