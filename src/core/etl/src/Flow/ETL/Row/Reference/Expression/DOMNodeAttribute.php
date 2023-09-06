<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class DOMNodeAttribute implements Expression
{
    public function __construct(private readonly Expression $ref, private readonly string $attribute)
    {
    }

    public function eval(Row $row) : null|string
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!$value instanceof \DOMNode) {
            return null;
        }

        if (!$value->hasAttributes()) {
            return null;
        }

        $attributes = $value->attributes;

        /**
         * @psalm-suppress PossiblyNullReference
         *
         * @phpstan-ignore-next-line
         */
        if (!$attributes->getNamedItem($this->attribute)) {
            return null;
        }

        /**
         * @psalm-suppress PossiblyNullPropertyFetch
         *
         * @phpstan-ignore-next-line
         */
        return $attributes->getNamedItem($this->attribute)->nodeValue;
    }
}
