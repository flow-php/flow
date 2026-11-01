<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Frame;

/**
 * Only the no-op codec (0x00) is supported; its id is stamped into the header flags byte.
 */
interface Codec
{
    public function decode(string $data): string;

    public function encode(string $data): string;

    public function id(): int;
}
