<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_uuid;
use Flow\ETL\PHP\Type\Caster\StringTypeChecker;

final class AutoCaster
{
    public function __construct(private readonly Caster $caster)
    {
    }

    public function cast(mixed $value) : mixed
    {
        if (!\is_string($value)) {
            return $value;
        }

        $typeChecker = new StringTypeChecker($value);

        if ($typeChecker->isNull()) {
            return null;
        }

        if ($typeChecker->isInteger()) {
            return $this->caster->to(type_integer())->value($value);
        }

        if ($typeChecker->isFloat()) {
            return $this->caster->to(type_float())->value($value);
        }

        if ($typeChecker->isBoolean()) {
            return $this->caster->to(type_boolean())->value($value);
        }

        if ($typeChecker->isJson()) {
            return $this->caster->to(type_json())->value($value);
        }

        if ($typeChecker->isUuid()) {
            return $this->caster->to(type_uuid())->value($value);
        }

        if ($typeChecker->isDateTime()) {
            return $this->caster->to(type_datetime())->value($value);
        }

        return $value;
    }
}
