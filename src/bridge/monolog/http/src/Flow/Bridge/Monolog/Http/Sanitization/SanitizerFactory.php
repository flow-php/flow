<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Sanitization;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;

final class SanitizerFactory
{
    /**
     * Create a sanitizer from an array representation.
     *
     * @param array<string, mixed> $data
     *
     * @throws InvalidArgumentException When the sanitizer type is not supported or required data is missing
     */
    public static function fromArray(array $data) : Sanitizer
    {
        if (!isset($data['type'])) {
            throw new InvalidArgumentException('Sanitizer type is required');
        }

        return match ($data['type']) {
            'mask' => Mask::fromArray($data),
            default => throw new InvalidArgumentException(\sprintf('Unsupported sanitizer type: %s', $data['type'])),
        };
    }
}
