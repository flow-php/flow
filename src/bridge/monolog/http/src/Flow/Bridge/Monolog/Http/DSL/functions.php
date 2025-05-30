<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\DSL;

use Flow\Bridge\Monolog\Http\Sanitization\Mask;

/**
 * Create a new Mask sanitizer.
 *
 * @param string $character - character used for masking
 * @param int $offset - start masking values from this offset
 */
function mask(string $character = '*', int $offset = 0) : Mask
{
    return new Mask($character, $offset);
}
