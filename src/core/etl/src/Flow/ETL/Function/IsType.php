<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\PHP\Type\{Type, TypeFactory};
use Flow\ETL\Row;

final class IsType extends ScalarFunctionChain
{
    /**
     * @var array<string|Type<mixed>>
     */
    private readonly array $types;

    /**
     * @param string|Type<mixed> ...$types
     */
    public function __construct(
        private readonly mixed $value,
        string|Type ...$types,
    ) {

        $this->types = $types;
    }

    public function eval(Row $row) : bool
    {
        $value = (new Parameter($this->value))->eval($row);

        foreach ($this->types as $type) {
            $type = \is_string($type) ? TypeFactory::fromString($type) : $type;

            if ($type->isValid($value)) {
                return true;
            }
        }

        return false;
    }
}
