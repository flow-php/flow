<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Rows;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;

final class ExtRowFrameEncoder implements RowFrameEncoder
{
    private readonly RowsEncoder $encoder;

    public function __construct()
    {
        $this->encoder = new RowsEncoder();
    }

    public function encode(Rows $rows): array
    {
        try {
            return $this->encoder->rows($rows);
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }
}
