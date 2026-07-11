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
 * Stateful Floe frame decoder for streaming reads - the PHP side keeps
 * buffering/framing and hands over bare SCHEMA/ROW frame bodies.
 */
final class RowsDecoder
{
    public function __construct()
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @throws ExtensionException
     */
    public function row(string $frameBody): Row
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param array<string> $frameBodies
     *
     * @throws ExtensionException
     */
    public function rows(array $frameBodies): Rows
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
