<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\ValueConverter;

/**
 * @psalm-immutable
 */
final class DateTimeToStringCaster implements ValueConverter
{
    private string $format;

    public function __construct(string $format = \DateTimeInterface::ATOM)
    {
        $this->format = $format;
    }

    /**
     * @return array{format: string}
     */
    public function __serialize() : array
    {
        return [
            'format' => $this->format,
        ];
    }

    /**
     * @param array{format: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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
