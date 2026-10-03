<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Flow\ETL\Column\Php\ListColumnBuilder;
use Flow\ETL\Column\Php\MapColumnBuilder;
use Flow\ETL\Column\Php\NullColumnBuilder;
use Flow\ETL\Column\Php\PhpColumnBuilder;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Php\StructColumnBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\NullType;

final readonly class PhysicalBuilderFor
{
    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function type(Type $type): PhpColumnBuilder
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        if ($base instanceof ListType) {
            return new ListColumnBuilder($type, $this->type($base->element()));
        }

        if ($base instanceof MapType) {
            return new MapColumnBuilder($type, $this->type($base->key()), $this->type($base->value()));
        }

        if ($base instanceof StructureType) {
            $children = [];

            foreach ($base->elements() as $element) {
                $children[$element->name] = $this->type($element->type);
            }

            return new StructColumnBuilder($type, $children);
        }

        if ($base instanceof NullType) {
            return new NullColumnBuilder($type);
        }

        return new ScalarColumnBuilder($type, (new PhysicalFor())->type($type));
    }
}
