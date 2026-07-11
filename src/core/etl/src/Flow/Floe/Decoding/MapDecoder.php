<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function unpack;

final class MapDecoder implements ValueDecoder
{
    public function __construct(
        private readonly ValueDecoder $key,
        private readonly ValueDecoder $value,
    ) {}

    /**
     * @return array<array-key, mixed>
     */
    public function decode(string $data, int &$position): array
    {
        $count = unpack('V', $data, $position)[1];
        $position += 4;
        $values = [];

        for ($i = 0; $i < $count; $i++) {
            /** @var array-key $key */
            $key = $this->key->decode($data, $position);
            $values[$key] = $this->value->decode($data, $position);
        }

        return $values;
    }
}
