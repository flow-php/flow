<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Exception;

final class InvalidBuilderStateException extends \InvalidArgumentException
{
    public static function emptyConditionBuilder() : self
    {
        return new self('Cannot use empty ConditionBuilder in where clause, use isEmpty() to check before passing to where()');
    }

    public static function mutuallyExclusiveOptions(string $option1, string $option2) : self
    {
        return new self(\sprintf(
            'Cannot combine %s with %s in a single statement',
            $option1,
            $option2
        ));
    }

    public static function noOperationSpecified(string $builderName) : self
    {
        return new self(\sprintf(
            'No operation specified for %s',
            $builderName
        ));
    }
}
