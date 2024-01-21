<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_xml;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\XMLType;
use Flow\ETL\PHP\Type\Type;

final class XMLCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof XMLType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        if (\is_string($value)) {
            $doc = new \DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new CastingException($value, type_xml());
            }

            return $doc;
        }

        if ($value instanceof \DOMDocument) {
            return $value;
        }

        throw new CastingException($value, $type);
    }
}
