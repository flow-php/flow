<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\Parser\ParsedExcludeDefinition;

final class ParsedExcludeDefinitionMother
{
    /**
     * @param list<array{expression: string, operator: string}> $elements
     */
    public static function with(
        string $accessMethod = 'btree',
        array $elements = [['expression' => 'a', 'operator' => '=']],
        ?string $predicate = null,
        bool $deferrable = false,
        bool $initiallyDeferred = false,
    ): ParsedExcludeDefinition {
        return new ParsedExcludeDefinition($accessMethod, $elements, $predicate, $deferrable, $initiallyDeferred);
    }
}
