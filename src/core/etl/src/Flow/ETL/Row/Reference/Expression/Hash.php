<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Hash implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $algorithm = 'xxh128',
        private readonly bool $binary = false,
        private readonly array $options = []
    ) {
        if (!\in_array($this->algorithm, \hash_algos(), true)) {
            throw new \InvalidArgumentException(\sprintf('Hash algorithm "%s" is not supported', $this->algorithm));
        }
    }

    public function eval(Row $row) : ?string
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match ($value) {
            null => null,
            default => match (\gettype($value)) {
                'array', 'object' => \hash($this->algorithm, \serialize($value), $this->binary, $this->options),
                default => \hash($this->algorithm, (string) $value, $this->binary, $this->options),
            }
        };
    }
}
