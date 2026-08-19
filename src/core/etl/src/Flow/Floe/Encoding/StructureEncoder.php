<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\Format;

use function array_key_exists;

/**
 * @implements ValueEncoder<array<array-key, mixed>>
 */
final class StructureEncoder implements ValueEncoder
{
    /**
     * @param array<array-key, ValueEncoder<mixed>> $elements structure element name => encoder
     */
    public function __construct(
        private readonly array $elements,
    ) {}

    public function encode(mixed $value): string
    {
        $buffer = '';

        foreach ($this->elements as $name => $element) {
            if (!array_key_exists($name, $value)) {
                $buffer .= Format::VALUE_ABSENT_BYTE;
            } elseif ($value[$name] === null) {
                $buffer .= Format::VALUE_NULL_BYTE;
            } else {
                $buffer .= Format::VALUE_PRESENT_BYTE . $element->encode($value[$name]);
            }
        }

        return $buffer;
    }
}
