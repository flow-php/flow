<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array{format: string}>
 */
final class DateTimeToStringCaster implements ValueConverter
{
    public function __construct(private string $format = \DateTimeInterface::ATOM)
    {
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

    public function convert(mixed $value) : string
    {
        if (!$value instanceof \DateTimeInterface) {
            throw new InvalidArgumentException('Only \DateTimeInterface can be casted to string, got ' . \gettype($value));
        }

        return $value->format($this->format);
    }
}
