<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Exception;

use InvalidArgumentException;

use function implode;
use function sprintf;

final class InvalidBuilderStateException extends InvalidArgumentException
{
    public static function emptyConditionBuilder(): self
    {
        return new self(
            'Cannot use empty ConditionBuilder in where clause, use isEmpty() to check before passing to where()',
        );
    }

    public static function mutuallyExclusiveOptions(string $option1, string $option2): self
    {
        return new self(sprintf('Cannot combine %s with %s in a single statement', $option1, $option2));
    }

    public static function noOperationSpecified(string $builderName): self
    {
        return new self(sprintf('No operation specified for %s', $builderName));
    }

    public static function returningAliasesWithoutList(?string $oldAlias, ?string $newAlias): self
    {
        $aliases = [];

        if ($oldAlias !== null) {
            $aliases[] = 'OLD AS ' . $oldAlias;
        }

        if ($newAlias !== null) {
            $aliases[] = 'NEW AS ' . $newAlias;
        }

        return new self(sprintf('RETURNING WITH (%s) needs a RETURNING list. Call returning() or returningAll() after returningAliases().', implode(
            ', ',
            $aliases,
        )));
    }

    public static function recursiveWithOnMerge(): self
    {
        return new self(
            'MERGE does not accept WITH RECURSIVE, because PostgreSQL rejects it. Remove recursive() from the WITH clause of this MERGE, or use INSERT, UPDATE or DELETE.',
        );
    }
}
