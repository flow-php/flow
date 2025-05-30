<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Sanitization;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;

final readonly class Mask implements Sanitizer
{
    /**
     * @param string $character - character used for masking
     * @param int $offset - start masking values from this offset
     */
    public function __construct(
        private string $character = '*',
        private int $offset = 0,
    ) {
    }

    /**
     * Create a Mask sanitizer from an array representation.
     *
     * @param array<string, mixed> $data
     *
     * @throws InvalidArgumentException When required data is missing or invalid
     */
    public static function fromArray(array $data) : self
    {
        $character = $data['character'] ?? '*';
        $offset = $data['offset'] ?? 0;

        if (!\is_string($character)) {
            throw new InvalidArgumentException('Character must be a string');
        }

        if (!\is_int($offset)) {
            throw new InvalidArgumentException('Offset must be an integer');
        }

        return new self($character, $offset);
    }

    /**
     * {@inheritdoc}
     */
    public function normalize() : array
    {
        return [
            'type' => 'mask',
            'character' => $this->character,
            'offset' => $this->offset,
        ];
    }

    public function sanitize(string $value) : string
    {
        if ($value === '') {
            return '';
        }

        $length = \strlen($value);

        if ($this->offset >= $length) {
            return $value;
        }

        return \substr($value, 0, $this->offset) . \str_repeat($this->character, $length - $this->offset);
    }
}
