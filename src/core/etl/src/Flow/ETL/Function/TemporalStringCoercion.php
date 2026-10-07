<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Native\StringType;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

final readonly class TemporalStringCoercion
{
    public function coerce(FunctionTree $function): FunctionTree
    {
        if (!$function instanceof ComparisonFunction) {
            return $function;
        }

        $operands = $function->operands();
        $types = [];

        try {
            foreach ($operands as $operand) {
                if (!$operand->resolved()) {
                    return $function;
                }

                $types[] = $operand->returns();
            }
        } catch (SchemaNotDerivableException) {
            return $function;
        }

        $temporal = null;

        foreach ($types as $type) {
            $bare = type_bare($type);
            $candidate = $bare instanceof ListType ? type_bare($bare->element()) : $bare;

            if ($candidate instanceof DateTimeType || $candidate instanceof DateType) {
                $temporal = $candidate;

                break;
            }
        }

        if ($temporal === null) {
            return $function;
        }

        $coerced = false;

        foreach ($types as $i => $type) {
            $bare = type_bare($type);

            if ($bare instanceof StringType) {
                $operands[$i] = new Cast($operands[$i], $this->nullableAs($temporal, $type));
                $coerced = true;
            } elseif ($bare instanceof ListType && type_bare($bare->element()) instanceof StringType) {
                $operands[$i] = new Cast($operands[$i], $this->nullableAs(
                    type_list($this->nullableAs($temporal, $bare->element())),
                    $type,
                ));
                $coerced = true;
            }
        }

        return $coerced ? $function->withOperands($operands) : $function;
    }

    /**
     * @param Type<mixed> $type
     * @param Type<mixed> $operand
     *
     * @return Type<mixed>
     */
    private function nullableAs(Type $type, Type $operand): Type
    {
        return type_is_nullable($operand) ? type_optional($type) : $type;
    }
}
