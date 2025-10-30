<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native\String;

use function Flow\Types\DSL\{type_html, type_json, type_uuid, type_xml};
use Flow\Types\Type;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeNarrower;

final readonly class StringTypeNarrower implements TypeNarrower
{
    public function narrow(Type $type, mixed $value) : Type
    {
        if (!$type instanceof StringType || !\is_string($value)) {
            return $type;
        }

        $checker = new StringTypeChecker($value);

        if ($checker->isJson()) {
            return type_json();
        }

        if ($checker->isUuid()) {
            return type_uuid();
        }

        if ($checker->isHTML()) {
            return type_html();
        }

        if ($checker->isXML()) {
            return type_xml();
        }

        return $type;
    }
}
