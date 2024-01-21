<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Type;

final class ListCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ListType;
    }

    public function value(mixed $value, Type $type, Caster $caster) : mixed
    {
        /** @var ListType $type */
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if (!\is_array($value)) {
                return [$caster->to($type->element()->type())->value($value)];
            }

            $castedList = [];

            foreach ($value as $key => $item) {
                $castedList[$key] = $caster->to($type->element()->type())->value($item);
            }

            return $castedList;
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
