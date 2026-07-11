<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;

use function ord;
use function sprintf;
use function substr;
use function unpack;

final class StructureDecoder implements ValueDecoder
{
    /**
     * @param array<array-key, ValueDecoder> $elements structure element name => decoder
     */
    public function __construct(
        private readonly array $elements,
        private readonly bool $allowsExtra,
        private readonly DynamicDecoder $dynamic,
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

        if ($this->allowsExtra) {
            $count = unpack('V', $data, $position)[1];
            $position += 4;

            for ($i = 0; $i < $count; $i++) {
                $length = unpack('V', $data, $position)[1];
                $key = substr($data, $position + 4, $length);
                $position += 4 + $length;
                $structure[$key] = $this->dynamic->decode($data, $position);
            }
        }

        return $structure;
    }
}
