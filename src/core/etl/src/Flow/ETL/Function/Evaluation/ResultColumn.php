<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Evaluation;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Function\ScalarFunction;
use Flow\Types\Type;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function get_debug_type;

final readonly class ResultColumn
{
    public function __construct(
        private Backend $backend,
    ) {}

    /**
     * The `list<returns()>` column of an ExpandResults function: one list per row.
     *
     * @param list<?list<mixed>> $lists
     */
    public function lists(ScalarFunction $function, array $lists): Column
    {
        $element = ReturnTypes::of($function);

        if ($element === null) {
            return new ValueColumn($lists);
        }

        return $this->typed(type_optional(type_list($element)), $lists);
    }

    /**
     * @param list<mixed> $values logical or raw, cast once by the builder
     */
    public function of(ScalarFunction $function, array $values): Column
    {
        $returns = ReturnTypes::of($function);

        if ($returns === null) {
            return new ValueColumn($values);
        }

        // nullable on purpose: only the step that stores the column (withEntry/filter gate) enforces NOT NULL, as today
        return $this->typed($returns instanceof NullType ? $returns : type_optional(type_bare($returns)), $values);
    }

    /**
     * An intermediate keeps its values in a typed column only when that column holds them losslessly: a type with a
     * column kind, no xml/xml_element/html/html_element (a column keeps markup, not the live node) and no untyped
     * array<mixed> (a column keeps it as json) at any depth.
     *
     * @param Type<mixed> $type
     */
    public function holdsLosslessly(Type $type): bool
    {
        return (new PhysicalFor())->supports($type) && $this->keepsValues($type);
    }

    /**
     * @param Type<mixed> $type
     */
    public function keepsValues(Type $type): bool
    {
        $base = type_bare($type);

        if (
            $base instanceof ArrayType
            || $base instanceof XMLType
            || $base instanceof XMLElementType
            || $base instanceof HTMLType
            || $base instanceof HTMLElementType
        ) {
            return false;
        }

        if ($base instanceof ListType) {
            return $this->keepsValues($base->element());
        }

        if ($base instanceof MapType) {
            return $this->keepsValues($base->value());
        }

        if ($base instanceof StructureType) {
            foreach ($base->elements() as $element) {
                if (!$this->keepsValues($element->type)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $values
     */
    public function typed(Type $type, array $values): Column
    {
        $bare = type_bare($type);
        $native = match ($bare::class) {
            BooleanType::class => 'bool',
            IntegerType::class => 'int',
            FloatType::class => 'float',
            StringType::class => 'string',
            default => null,
        };

        // values that already hold the column's PHP type are its physicals: no definition, no casting builder
        if ($native !== null && $type instanceof OptionalType) {
            $nulls = 0;

            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $value) {
                if ($value === null) {
                    $nulls++;
                } elseif (get_debug_type($value) !== $native) {
                    $native = null;

                    break;
                }
            }

            if ($native !== null) {
                $builder = $this->backend->builder(definition_from_type('value', $type));
                $builder->appendPhysicals($values, $nulls);

                return $builder->finish();
            }
        }

        if (!$this->holdsLosslessly($type)) {
            return new ValueColumn($values);
        }

        $builder = $this->backend->builder(definition_from_type('value', $type));
        $builder->appendMany($values);

        return $builder->finish();
    }
}
