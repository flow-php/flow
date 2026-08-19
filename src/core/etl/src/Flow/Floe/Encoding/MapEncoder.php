<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function count;
use function pack;

/**
 * @implements ValueEncoder<array<array-key, mixed>>
 */
final class MapEncoder implements ValueEncoder
{
    public function __construct(
        private readonly ValueEncoder $key,
        private readonly ValueEncoder $value,
    ) {}

    public function encode(mixed $value): string
    {
        $buffer = pack('V', count($value));

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            $buffer .= $this->key->encode($key) . $this->value->encode($item);
        }

        return $buffer;
    }
}
