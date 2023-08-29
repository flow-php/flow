<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

if (!\class_exists(\Symfony\Component\Uid\Ulid::class)) {
    throw new RuntimeException("\Symfony\Component\Uid\Ulid class not found, please add 'symfony/uid' as a dependency to the project first.");
}

final class Ulid implements Expression
{
    public function __construct(private readonly ?Expression $ref = null)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $param */
        $param = $this->ref?->eval($row);

        if (null !== $param) {
            if (!\is_string($param)) {
                return null;
            }

            try {
                return \Symfony\Component\Uid\Ulid::fromString($param);
            } catch (\InvalidArgumentException) {
                return null;
            }
        }

        return new \Symfony\Component\Uid\Ulid();
    }
}
