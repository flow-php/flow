<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array{format: string}>
 * @psalm-immutable
 */
final class DateTimeToStringCaster implements ValueConverter
{
    private string $format;

    public function __construct(string $format = \DateTimeInterface::ATOM)
    {
        $this->format = $format;
    }

    public function __serialize() : array
    {
        return [
            'format' => $this->format,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->format = $data['format'];
    }

    public function convert($value) : string
    {
        if (!$value instanceof \DateTimeInterface) {
            throw new InvalidArgumentException('Only \DateTimeInterface can be casted to string, got ' . \gettype($value));
        }

        /** @psalm-suppress ImpureMethodCall */
        return $value->format($this->format);
    }
}
