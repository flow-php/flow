<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\Types\Type\{Type, TypeDetector};

final class InvalidTypeException extends InvalidArgumentException
{
    private function __construct(string $message, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    /**
     * @param Type<mixed> $givenType
     * @param Type<mixed> $expectedType
     */
    public static function type(Type $givenType, Type $expectedType) : self
    {
        return new self(
            sprintf(
                'Expected type "%s", got "%s".',
                $expectedType->toString(),
                $givenType->toString(),
            )
        );
    }

    /**
     * @param Type<mixed> $expectedType
     */
    public static function value(mixed $value, Type $expectedType) : self
    {
        return new self(
            sprintf(
                'Expected type "%s", got "%s".',
                $expectedType->toString(),
                (new TypeDetector())->detectType($value)->toString(),
            )
        );
    }
}
