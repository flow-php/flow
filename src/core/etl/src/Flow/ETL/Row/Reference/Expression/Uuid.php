<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

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

    public static function uuid8(?Expression $ref = null) : self
    {
        return new self('uuid8', $ref);
    }

    public function eval(Row $row) : mixed
    {
        if ($this->uuidVersion==='uuid4') {
            return \Ramsey\Uuid\Uuid::uuid4();
        }

        /** @var mixed $value */
        $param = $this->ref->eval($row);

        if ($this->uuidVersion==='uuid7') {
            return \Ramsey\Uuid\Uuid::uuid7($param);
        }

        if (null === $param) {
            return null;
        }

        return \Ramsey\Uuid\Uuid::uuid8($param);
    }
}
