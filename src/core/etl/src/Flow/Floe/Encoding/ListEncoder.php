<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function count;
use function pack;

final class ListEncoder implements ValueEncoder
{
    public function __construct(
        private readonly ValueEncoder $element,
    ) {}

    public function encode(mixed $value): string
    {
        /** @var array<int, mixed> $value */
        $buffer = pack('V', count($value));

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $item) {
            $buffer .= $this->element->encode($item);
        }

        return $buffer;
    }
}
