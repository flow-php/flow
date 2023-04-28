<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

if (!\class_exists(\Ramsey\Uuid\Uuid::class)) {
    throw new RuntimeException("\Ramsey\Uuid\Uuid class not found, please add ramsey/uuid dependency to the project first.");
}
final class Uuid implements Expression
{
    private function __construct(private readonly string $uuidVersion, private readonly ?Expression $ref = null)
    {
    }

    public static function uuid4() : self
    {
        return new self('uuid4');
    }

    public static function uuid7(?Expression $ref = null) : self
    {
        return new self('uuid7', $ref);
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $param */
        $param = $this->ref?->eval($row);

        return match ($this->uuidVersion) {
            'uuid4' => \Ramsey\Uuid\Uuid::uuid4(),
            'uuid7' => $param instanceof \DateTimeInterface?\Ramsey\Uuid\Uuid::uuid7($param):null,
            default=> null
        };
    }
}
