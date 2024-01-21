<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;

final class StringCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ScalarType && $type->isString();
    }

    public function value(mixed $value, Type $type, Caster $caster) : mixed
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        if (\is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (\is_array($value)) {
            return \json_encode($value, JSON_THROW_ON_ERROR);
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format(\DateTimeInterface::RFC3339);
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        if ($value instanceof \DOMDocument) {
            return $value->saveXML() ?: null;
        }

        try {
            return (string) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
