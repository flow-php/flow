<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use Throwable;

use function array_keys;
use function count;
use function function_exists;
use function in_array;
use function is_array;
use function ltrim;
use function mb_strrpos;
use function mb_substr;
use function str_starts_with;
use function token_get_all;

/**
 * A regex pass over the same fences produced 172 false positives from SQL identifiers inside string
 * literals, so the code is tokenized and only a T_STRING immediately followed by "(" is a call.
 */
final readonly class FenceSymbols
{
    /**
     * @return list<string>
     */
    public function unresolved(Fence $fence, DefinedFunctions $functions, DocumentDeclarations $declarations): array
    {
        $tokens = $this->tokenize($fence);

        if ($tokens === null) {
            return [];
        }

        $unresolved = [];

        foreach ($this->calls($tokens) as $name => $isMethod) {
            if ($declarations->declares($name) || function_exists($name)) {
                continue;
            }

            if ($isMethod || $functions->has($name)) {
                continue;
            }

            $unresolved[$name] = true;
        }

        return array_keys($unresolved);
    }

    public function tokenizes(Fence $fence): bool
    {
        return $this->tokenize($fence) !== null;
    }

    /**
     * @return null|list<array{0: int, 1: string, 2: int}|string>
     */
    private function tokenize(Fence $fence): ?array
    {
        $code = $fence->code;

        if (!str_starts_with(ltrim($code), '<?php')) {
            $code = "<?php\n" . $code;
        }

        try {
            /** @var list<array{0: int, 1: string, 2: int}|string> $tokens */
            $tokens = token_get_all($code, TOKEN_PARSE);
        } catch (Throwable) {
            return null;
        }

        return $tokens;
    }

    /**
     * @param list<array{0: int, 1: string, 2: int}|string> $tokens
     *
     * @return array<string, bool> call name => is it a method call
     */
    private function calls(array $tokens): array
    {
        $calls = [];
        $count = count($tokens);

        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];

            if (!is_array($token)) {
                continue;
            }

            if ($token[0] !== T_STRING && $token[0] !== T_NAME_QUALIFIED && $token[0] !== T_NAME_FULLY_QUALIFIED) {
                continue;
            }

            if (!$this->isFollowedByOpenParenthesis($tokens, $i)) {
                continue;
            }

            $previous = $this->previousMeaningful($tokens, $i);

            if ($previous !== null && in_array($previous, [T_FUNCTION, T_NEW, T_CLASS, T_CONST, T_ATTRIBUTE], true)) {
                continue;
            }

            $name = $token[1];
            $separator = mb_strrpos($name, '\\');

            if ($separator !== false) {
                $name = mb_substr($name, $separator + 1);
            }

            $calls[$name] = in_array($previous, [T_OBJECT_OPERATOR, T_NULLSAFE_OBJECT_OPERATOR, T_DOUBLE_COLON], true);
        }

        return $calls;
    }

    /**
     * @param list<array{0: int, 1: string, 2: int}|string> $tokens
     */
    private function isFollowedByOpenParenthesis(array $tokens, int $index): bool
    {
        $count = count($tokens);

        for ($i = $index + 1; $i < $count; $i++) {
            $next = $tokens[$i];

            if (
                is_array($next)
                && ($next[0] === T_WHITESPACE || $next[0] === T_COMMENT || $next[0] === T_DOC_COMMENT)
            ) {
                continue;
            }

            return $next === '(';
        }

        return false;
    }

    /**
     * @param list<array{0: int, 1: string, 2: int}|string> $tokens
     */
    private function previousMeaningful(array $tokens, int $index): int|string|null
    {
        for ($i = $index - 1; $i >= 0; $i--) {
            $token = $tokens[$i];

            if (is_array($token)) {
                if ($token[0] === T_WHITESPACE || $token[0] === T_COMMENT || $token[0] === T_DOC_COMMENT) {
                    continue;
                }

                return $token[0];
            }

            return $token;
        }

        return null;
    }
}
