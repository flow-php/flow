<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\Format;

use function array_diff_key;
use function array_key_exists;
use function count;
use function pack;
use function strlen;

final class StructureEncoder implements ValueEncoder
{
    /**
     * @param array<array-key, ValueEncoder> $elements structure element name => encoder
     */
    public function __construct(
        private readonly array $elements,
        private readonly bool $allowsExtra,
        private readonly DynamicEncoder $dynamic,
    ) {}

    public function encode(mixed $value): string
    {
        /** @var array<array-key, mixed> $value */
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

        if ($this->allowsExtra) {
            $extra = array_diff_key($value, $this->elements);
            $buffer .= pack('V', count($extra));

            // @mago-ignore analysis:mixed-assignment
            foreach ($extra as $key => $item) {
                $buffer .= pack('V', strlen((string) $key)) . $key . $this->dynamic->encode($item);
            }
        }

        return $buffer;
    }
}
