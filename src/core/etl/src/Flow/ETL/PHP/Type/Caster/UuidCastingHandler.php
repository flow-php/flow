<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry\Type\Uuid;

final class UuidCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof UuidType;
    }

    public function value(mixed $value, Type $type, Caster $caster) : mixed
    {
        if ($value instanceof Uuid) {
            return $value;
        }

        if (\is_string($value)) {
            return new Uuid($value);
        }

        if ($value instanceof \Ramsey\Uuid\UuidInterface) {
            return new Uuid($value);
        }

        if ($value instanceof \Symfony\Component\Uid\Uuid) {
            return new Uuid($value->toRfc4122());
        }

        throw new CastingException($value, $type);
    }
}
