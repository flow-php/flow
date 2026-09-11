<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Closure;
use Flow\Filesystem\SourceStream;
use Generator;

use function json_decode;
use function json_last_error;
use function preg_match;
use function str_starts_with;
use function strlen;
use function strspn;
use function substr;

use const JSON_ERROR_NONE;

/**
 * JSON Machine tokenizes a document in PHP, one character class at a time. The elements of a top-level array are
 * found here instead by one PCRE match per element - which runs in C - and decoded by json_decode(), about eight
 * times faster on the orders fixture. Anything this does not recognise goes to JSON Machine.
 */
final readonly class JsonArrayElements
{
    /**
     * One balanced object or array at the cursor. A string may hold any bracket; every quantifier is possessive, so
     * PCRE never backtracks into an element.
     */
    private const string ELEMENT = '/\G(\{(?:[^{}"]++|"(?:[^"\\\\]++|\\\\.)*+"|(?1))*+\}|\[(?:[^\[\]"]++|"(?:[^"\\\\]++|\\\\.)*+"|(?1))*+\])/s';

    /**
     * @param int<1, max> $chunk bytes read from the stream per refill
     */
    public function __construct(
        private int $chunk = 1 << 20,
    ) {}

    /**
     * $fallback yields the whole document's items through JSON Machine, skipping the given number of leading items -
     * so a scalar element, a malformed element, a missing separator or a document that is not an array yields or
     * refuses exactly what JSON Machine always did, and never repeats an item this already yielded.
     *
     * @param string $first the stream's first bytes, already read at offset 0
     * @param Closure(int): iterable<mixed> $fallback
     *
     * @return Generator<mixed, mixed>
     */
    public function of(SourceStream $stream, string $first, Closure $fallback): Generator
    {
        $buffer = $first;
        $read = strlen($first);
        $position = str_starts_with($buffer, "\u{FEFF}") ? 3 : 0;
        $eof = false;
        $refill = false;
        $opened = false;
        $afterElement = false;
        $afterComma = false;
        $yielded = 0;

        while (true) {
            if ($refill) {
                $refill = false;
                $more = $stream->read($this->chunk, $read);

                if ($more === '') {
                    $eof = true;
                } else {
                    $buffer = substr($buffer, $position) . $more;
                    $position = 0;
                    $read += strlen($more);
                }
            }

            $position += strspn($buffer, " \t\n\r", $position);

            if ($position >= strlen($buffer)) {
                if ($eof) {
                    yield from $fallback($yielded);

                    return;
                }

                $refill = true;

                continue;
            }

            $char = $buffer[$position];

            if (!$opened) {
                if ($char !== '[') {
                    yield from $fallback(0);

                    return;
                }

                $opened = true;
                $position++;

                continue;
            }

            // JSON Machine stops at the closing bracket and ignores whatever follows it
            if ($char === ']' && !$afterComma) {
                return;
            }

            if ($afterElement) {
                if ($char !== ',') {
                    yield from $fallback($yielded);

                    return;
                }

                $afterElement = false;
                $afterComma = true;
                $position++;

                continue;
            }

            if ($char !== '{' && $char !== '[') {
                yield from $fallback($yielded);

                return;
            }

            $match = [];
            $matched = preg_match(self::ELEMENT, $buffer, $match, 0, $position);

            if ($matched === 1) {
                // the arguments JSON Machine's ExtJsonDecoder(true) passes
                // @mago-ignore analysis:mixed-assignment
                $value = json_decode($match[0], true, 512);

                if (json_last_error() !== JSON_ERROR_NONE) {
                    yield from $fallback($yielded);

                    return;
                }

                yield $yielded => $value;

                $yielded++;
                $position += strlen($match[0]);
                $afterElement = true;
                $afterComma = false;

                continue;
            }

            // 0 with bytes left to read is an element the buffer cuts; false is a PCRE limit
            if ($matched === false || $eof) {
                yield from $fallback($yielded);

                return;
            }

            $refill = true;
        }
    }
}
