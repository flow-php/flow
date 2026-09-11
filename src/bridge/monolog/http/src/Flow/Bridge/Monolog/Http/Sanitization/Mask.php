<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Sanitization;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function str_repeat;
use function strlen;
use function substr;

final readonly class Mask implements Sanitizer
{
    /**
     * @param string $character - character used for masking
     * @param int $offset - start masking values from this offset
     */
    public function __construct(
        private string $character = '*',
        private int $offset = 0,
    ) {}

    /**
     * Create a Mask sanitizer from an array representation.
     *
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('mask'),
            'character' => structure_element('character', type_string(), optional: true),
            'offset' => structure_element('offset', type_integer(), optional: true),
        ])->assert($data);

        return new self($data['character'] ?? '*', $data['offset'] ?? 0);
    }

    /**
     * {@inheritdoc}
     */
    public function normalize(): array
    {
        return [
            'type' => 'mask',
            'character' => $this->character,
            'offset' => $this->offset,
        ];
    }

    public function sanitize(string $value): string
    {
        if ($value === '') {
            return '';
        }

        $length = strlen($value);

        if ($this->offset >= $length) {
            return $value;
        }

        return substr($value, 0, $this->offset) . str_repeat($this->character, max(0, $length - $this->offset));
    }
}
