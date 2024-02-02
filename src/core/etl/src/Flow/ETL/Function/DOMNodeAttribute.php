<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMNodeAttribute extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref, private readonly string $attribute)
    {
    }

    public function eval(Row $row) : string|null
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
