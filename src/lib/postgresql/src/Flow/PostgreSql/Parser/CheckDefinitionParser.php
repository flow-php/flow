<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use function ltrim;
use function stripos;
use function substr;

final readonly class CheckDefinitionParser
{
    public function __construct(
        private ExpressionParser $expressionParser,
    ) {}

    public function parse(string $definition): string
    {
        $expression = $this->stripWrapper($definition);
        $this->expressionParser->parse($expression);

        return $expression;
    }

    private function stripWrapper(string $definition): string
    {
        $trimmed = ltrim($definition);

        if (stripos($trimmed, 'CHECK (') !== 0) {
            return $definition;
        }

        if (!str_ends_with($trimmed, ')')) {
            return $definition;
        }

        return substr($trimmed, 7, -1);
    }
}
