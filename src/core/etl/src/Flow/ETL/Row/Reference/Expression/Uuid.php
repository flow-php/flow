<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Ramsey\Uuid\UuidInterface;

final class Uuid implements Expression
{
    private function __construct(private readonly UuidInterface $uuid)
    {
    }

    public static function uuid4() : self
    {
        return new self(\Ramsey\Uuid\Uuid::uuid4());
    }

    public static function uuid7(?\DateTimeInterface $dateTime = null) : self
    {
        return new self(\Ramsey\Uuid\Uuid::uuid7($dateTime));
    }

    public static function uuid8(string $bytes) : self
    {
        return new self(\Ramsey\Uuid\Uuid::uuid8($bytes));
    }

    public function eval(Row $row) : mixed
    {
        return $this->uuid->toString();
    }
}
