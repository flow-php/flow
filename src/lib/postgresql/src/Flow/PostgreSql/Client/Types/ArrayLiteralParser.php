<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\PostgreSql\Client\Exception\ValueConversionException;

use function array_pop;
use function preg_match;
use function strcspn;
use function strlen;
use function strtoupper;
use function substr;
use function trim;

/**
 * Parses a PostgreSQL array output literal into a PHP array.
 *
 * pg emits arrays as {a,b}; nested arrays as {{1,2},{3}}; an element is quoted when it contains a
 * comma, a brace, a quote, a backslash or leading/trailing whitespace, and inside quotes " and \
 * are backslash-escaped. An UNQUOTED NULL is the SQL null; the QUOTED "NULL" is the four-character
 * string. The empty array is {}.
 *
 * Element values come back as strings; typing them is ResultCaster's job.
 */
final readonly class ArrayLiteralParser
{
    /**
     * @throws ValueConversionException when the literal is not a well-formed array output literal
     *
     * @return list<mixed>
     */
    public function parse(string $literal): array
    {
        // pg prefixes the literal with explicit bounds whenever an array's lower bound is not 1,
        // e.g. [2:3]={1,2}. The bounds carry no type information Flow can hold, so they are skipped.
        $bounds = [];

        if (preg_match('/^\[[\d:,]+]=/', $literal, $bounds) === 1) {
            $literal = substr($literal, strlen($bounds[0]));
        }

        $length = strlen($literal);

        if ($length === 0 || $literal[0] !== '{') {
            throw ValueConversionException::invalidArrayLiteral($literal, 'it does not open with {');
        }

        $position = 1;
        /** @var list<list<mixed>> $stack */
        $stack = [];
        /** @var list<mixed> $current */
        $current = [];
        $afterElement = false;
        $afterComma = false;
        $closed = false;

        while ($position < $length) {
            $character = $literal[$position];

            if ($closed) {
                if (trim($character) === '') {
                    $position++;

                    continue;
                }

                throw ValueConversionException::invalidArrayLiteral($literal, 'it has trailing characters');
            }

            if ($character === '{') {
                if ($afterElement) {
                    throw ValueConversionException::invalidArrayLiteral($literal, 'a nested array must follow a comma');
                }

                $stack[] = $current;
                $current = [];
                $afterElement = false;
                $afterComma = false;
                $position++;

                continue;
            }

            if ($character === '}') {
                if ($afterComma) {
                    throw ValueConversionException::invalidArrayLiteral($literal, 'it has an empty trailing element');
                }

                if ($stack === []) {
                    $closed = true;
                } else {
                    $parent = array_pop($stack);
                    $parent[] = $current;
                    $current = $parent;
                    $afterElement = true;
                }

                $position++;

                continue;
            }

            if ($character === ',') {
                if (!$afterElement) {
                    throw ValueConversionException::invalidArrayLiteral($literal, 'it has an empty element');
                }

                $afterElement = false;
                $afterComma = true;
                $position++;

                continue;
            }

            if ($afterElement) {
                throw ValueConversionException::invalidArrayLiteral(
                    $literal,
                    'two elements are not separated by a comma',
                );
            }

            if ($character === '"') {
                $value = '';
                $position++;

                while (true) {
                    $span = strcspn($literal, '"\\', $position);
                    $value .= substr($literal, $position, $span);
                    $position += $span;

                    if ($position >= $length) {
                        throw ValueConversionException::invalidArrayLiteral(
                            $literal,
                            'it ends inside a quoted element',
                        );
                    }

                    if ($literal[$position] === '"') {
                        break;
                    }

                    $position++;

                    if ($position >= $length) {
                        throw ValueConversionException::invalidArrayLiteral($literal, 'it ends inside an escape');
                    }

                    $value .= $literal[$position];
                    $position++;
                }

                $current[] = $value;
                $position++;
                $afterElement = true;
                $afterComma = false;

                continue;
            }

            $span = strcspn($literal, ',}{', $position);
            $value = trim(substr($literal, $position, $span));
            $position += $span;

            if ($value === '') {
                throw ValueConversionException::invalidArrayLiteral($literal, 'it has an empty element');
            }

            $current[] = strtoupper($value) === 'NULL' ? null : $value;
            $afterElement = true;
            $afterComma = false;
        }

        if (!$closed) {
            throw ValueConversionException::invalidArrayLiteral($literal, 'it is not terminated');
        }

        return $current;
    }
}
