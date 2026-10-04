<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Evaluation;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ScalarFunction;
use Flow\Types\Type;
use WeakMap;

/**
 * A function tree is immutable, so each node's returns() is derived once and kept for as long as the node lives -
 * not once per evaluated batch.
 */
final class ReturnTypes
{
    /**
     * @var null|WeakMap<ScalarFunction, false|Type<mixed>> false: the result schema is not derivable
     */
    private static ?WeakMap $types = null;

    /**
     * @return null|Type<mixed> null when the function's result schema is not derivable
     */
    public static function of(ScalarFunction $function): ?Type
    {
        self::$types ??= new WeakMap();

        if (!self::$types->offsetExists($function)) {
            try {
                self::$types[$function] = $function->returns();
            } catch (SchemaNotDerivableException) {
                self::$types[$function] = false;
            }
        }

        $type = self::$types[$function];

        return $type === false ? null : $type;
    }
}
