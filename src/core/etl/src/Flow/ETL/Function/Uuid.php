<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;

if (!\class_exists(\Ramsey\Uuid\Uuid::class) && !\class_exists(\Symfony\Component\Uid\Uuid::class)) {
    throw new RuntimeException("\Ramsey\Uuid\Uuid nor \Symfony\Component\Uid\Uuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.");
}

final class Uuid implements ScalarFunction
{
    use EntryScalarFunction;

    private function __construct(private readonly string $uuidVersion, private readonly ?ScalarFunction $ref = null)
    {
    }

    public static function uuid4() : self
    {
        return new self('uuid4');
    }

    public static function uuid7(?ScalarFunction $ref = null) : self
    {
        return new self('uuid7', $ref);
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $param */
        $param = $this->ref?->eval($row);

        return match ($this->uuidVersion) {
            'uuid4' => $this->generateV4(),
            'uuid7' => $param instanceof \DateTimeInterface ? $this->generateV7($param) : null,
            default => null
        };
    }

    private function generateV4() : \Symfony\Component\Uid\UuidV4|\Ramsey\Uuid\UuidInterface
    {
        if (\class_exists(\Ramsey\Uuid\Uuid::class)) {
            return \Ramsey\Uuid\Uuid::uuid4();
        }

        return \Symfony\Component\Uid\UuidV4::v4();
    }

    private function generateV7(\DateTimeInterface $dateTime) : \Symfony\Component\Uid\UuidV7|\Ramsey\Uuid\UuidInterface
    {
        if (\class_exists(\Ramsey\Uuid\Uuid::class)) {
            return \Ramsey\Uuid\Uuid::uuid7($dateTime);
        }

        return new \Symfony\Component\Uid\UuidV7(\Symfony\Component\Uid\UuidV7::generate($dateTime));
    }
}
