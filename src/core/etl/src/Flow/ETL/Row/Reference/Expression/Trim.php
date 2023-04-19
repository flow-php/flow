<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Trim implements Expression
{
    public const BOTH = 0;

    public const LEFT = 1;

    public const RIGHT = 2;

    /**
     * @param int<0, 2> $type
     */
    public function __construct(
        private readonly Expression $ref,
        private readonly int $type = self::BOTH,
        private readonly string $characters = " \t\n\r\0\x0B"
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match (\gettype($value)) {
            'string' => match ($this->type) {
                self::BOTH => \trim($value, $this->characters),
                self::LEFT => \ltrim($value, $this->characters),
                self::RIGHT => \rtrim($value, $this->characters),
            },
            default => null
        };
    }
}
