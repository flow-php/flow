<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\dom_element_to_string;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Native\StringType, Type};

final class StringCastingHandler implements CastingHandler
{
    /**
     * @param Type<string> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof StringType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : string
    {
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
            return $value->saveXML() ?: '';
        }

        if ($value instanceof \DOMElement) {
            return (string) dom_element_to_string($value);
        }

        try {
            return (string) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
