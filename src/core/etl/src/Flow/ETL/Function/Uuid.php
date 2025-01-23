<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_object, type_string, type_uuid};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\PHP\Value\Uuid as FlowUuid;
use Flow\ETL\Row;
use Ramsey\Uuid\UuidInterface;
use Symfony\Component\Uid\{UuidV4, UuidV7};

if (!\class_exists(\Ramsey\Uuid\Uuid::class) && !\class_exists(\Symfony\Component\Uid\Uuid::class)) {
    throw new RuntimeException("\Ramsey\Uuid\Uuid nor \Symfony\Component\Uid\Uuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.");
}

final class Uuid extends ScalarFunctionChain implements TypedScalarFunction
{
    private function __construct(
        private readonly ScalarFunction|string $uuidVersion,
        private readonly ScalarFunction|\DateTimeInterface|null $value = null,
    ) {
    }

    public static function uuid4() : self
    {
        return new self('uuid4');
    }

    public static function uuid7(ScalarFunction|\DateTimeInterface|null $value = null) : self
    {
        return new self('uuid7', $value);
    }

    public function eval(Row $row) : mixed
    {
        $param = (new Parameter($this->value))->as($row, type_string(), type_object(\DateTimeInterface::class));

        $uuidVersion = (new Parameter($this->uuidVersion))->asString($row);

        return match ($uuidVersion) {
            'uuid4' => new FlowUuid($this->generateV4()),
            'uuid7' => $param instanceof \DateTimeInterface ? new FlowUuid($this->generateV7($param)) : null,
            default => null,
        };
    }

    public function returns() : Type
    {
        return type_uuid();
    }

    private function generateV4() : UuidV4|UuidInterface
    {
        if (\class_exists(\Ramsey\Uuid\Uuid::class)) {
            return \Ramsey\Uuid\Uuid::uuid4();
        }

        return UuidV4::v4();
    }

    private function generateV7(\DateTimeInterface $dateTime) : UuidV7|UuidInterface
    {
        if (\class_exists(\Ramsey\Uuid\Uuid::class)) {
            return \Ramsey\Uuid\Uuid::uuid7($dateTime);
        }

        return new UuidV7(UuidV7::generate($dateTime));
    }
}
