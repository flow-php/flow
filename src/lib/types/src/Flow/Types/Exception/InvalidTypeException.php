<?php

declare(strict_types=1);

namespace Flow\Types\Exception;

use Flow\Types\Type;
use Flow\Types\Type\TypeDetector;
use Throwable;

final class InvalidTypeException extends InvalidArgumentException
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    /**
     * @param Type<mixed> $givenType
     * @param Type<mixed> $expectedType
     */
    public static function type(Type $givenType, Type $expectedType): self
    {
        return new self(sprintf('Expected type "%s", got "%s".', $expectedType->toString(), $givenType->toString()));
    }

    /**
     * @param Type<mixed> ...$types
     */
    public static function noCommonType(Type ...$types): self
    {
        return new self(sprintf('Cannot combine types "%s" - an explicit cast is required.', implode('", "', array_map(
            static fn(Type $type): string => $type->toString(),
            $types,
        ))));
    }

    /**
     * @param Type<mixed> $expectedType
     */
    public static function value(mixed $value, Type $expectedType): self
    {
        return new self(sprintf(
            'Expected type "%s", got "%s".',
            $expectedType->toString(),
            (new TypeDetector())
                ->detectType($value)
                ->toString(),
        ));
    }
}
