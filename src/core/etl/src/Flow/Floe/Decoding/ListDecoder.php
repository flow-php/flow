<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function unpack;

final class ListDecoder implements ValueDecoder
{
    public function __construct(
        private readonly ValueDecoder $element,
    ) {}

    /**
     * @return array<int, mixed>
     */
    public function decode(string $data, int &$position): array
    {
        $count = unpack('V', $data, $position)[1];
        $position += 4;
        $values = [];

        for ($i = 0; $i < $count; $i++) {
            $values[] = $this->element->decode($data, $position);
        }

        return $values;
    }
}
