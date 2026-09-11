<?php

declare(strict_types=1);

namespace Flow\Types\Exception;

use Flow\Types\Type;
use Throwable;

use function get_debug_type;
use function sprintf;

final class CastingException extends RuntimeException
{
    /**
     * @param mixed $value
     * @param Type<mixed> $type
     * @param null|\Throwable $previous
     * @param null|string $reason
     */
    public function __construct(
        public readonly mixed $value,
        public readonly Type $type,
        ?Throwable $previous = null,
        public readonly ?string $reason = null,
    ) {
        parent::__construct(
            $reason === null
                ? sprintf("Can't cast \"%s\" into \"%s\" type", get_debug_type($value), $type->toString())
                : sprintf("Can't cast \"%s\" into \"%s\" type: %s", get_debug_type($value), $type->toString(), $reason),
            0,
            $previous,
        );
    }
}
