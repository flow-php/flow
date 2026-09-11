<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;

use function ord;
use function sprintf;

final class StructureDecoder implements ValueDecoder
{
    /**
     * @param array<array-key, ValueDecoder> $elements structure element name => decoder
     */
    public function __construct(
        private readonly array $elements,
    ) {}

    /**
     * @return array<array-key, mixed>
     */
    public function decode(string $data, int &$position): array
    {
        $structure = [];

        foreach ($this->elements as $name => $element) {
            $flag = ord($data[$position++]);

            if ($flag === Format::VALUE_PRESENT) {
                $structure[$name] = $element->decode($data, $position);
            } elseif ($flag === Format::VALUE_NULL) {
                $structure[$name] = null;
            } elseif ($flag !== Format::VALUE_ABSENT) {
                throw new FloeException(sprintf('Floe found unknown structure element flag 0x%02X', $flag));
            }
        }

        return $structure;
    }
}
