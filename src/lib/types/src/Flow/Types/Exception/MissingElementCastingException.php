<?php

declare(strict_types=1);

namespace Flow\Types\Exception;

use Flow\Types\Type;
use Throwable;

use function get_debug_type;
use function sprintf;

final class MissingElementCastingException extends RuntimeException
{
    /**
     * @param mixed $value
     * @param Type<mixed> $type
     * @param string $element
     * @param null|\Throwable $previous
     */
    public function __construct(
        public readonly mixed $value,
        public readonly Type $type,
        public readonly string $element,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            sprintf(
                "Can't cast \"%s\" into \"%s\" type: element \"%s\" cannot be null",
                get_debug_type($value),
                $type->toString(),
                $element,
            ),
            0,
            $previous,
        );
    }
}
