<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\BlockCachedSourceStream;

use function str_contains;
use function str_replace;
use function strlen;
use function strpos;

/**
 * A newline after a cut is accepted once $confirmations consecutive records tokenise to $expectedFields fields.
 */
final readonly class CSVRecordResync
{
    private const int CHUNK = 1 << 12;

    private const int FIELD_START = 0;

    private const int UNENCLOSED = 1;

    private const int ENCLOSED = 2;

    private const int AFTER_ENCLOSURE = 3;

    private string $blanks;

    /**
     * @var int<1, max>
     */
    private int $confirmations;

    /**
     * @var int<1, max>
     */
    private int $expectedFields;

    public function __construct(
        private CSVDialect $dialect,
        int $expectedFields,
        int $confirmations = 3,
    ) {
        if ($expectedFields < 1) {
            throw new InvalidArgumentException('Expected fields must be greater than 0, given: ' . $expectedFields);
        }

        if ($confirmations < 1) {
            throw new InvalidArgumentException('Confirmations must be greater than 0, given: ' . $confirmations);
        }

        // what fgetcsv skips before an opening enclosure, minus the line endings that end a record here
        $this->blanks = str_replace($dialect->separator, '', " \t\v\f");
        $this->expectedFields = $expectedFields;
        $this->confirmations = $confirmations;
    }

    /**
     * The first byte offset at or after $from where a record provably starts; null when none does. Offset 0 starts
     * one by definition. Monotonic in $from, so neighbouring windows cut at after(offset) share every boundary.
     *
     * @param int<0, max> $from
     *
     * @return null|int<0, max>
     */
    public function after(SourceStream $stream, int $from): ?int
    {
        if ($from === 0) {
            return $stream->read(1, 0) === '' ? null : 0;
        }

        // candidates are probed a few hundred bytes at a time, walking forward - one inner read per block, not per probe
        $stream = new BlockCachedSourceStream($stream);
        $search = $from - 1;

        while (true) {
            $newline = null;

            while (($chunk = $stream->read(self::CHUNK, $search)) !== '') {
                $found = strpos($chunk, "\n");

                if ($found !== false) {
                    $newline = $search + $found;

                    break;
                }

                $search += strlen($chunk);
            }

            if ($newline === null) {
                return null;
            }

            $candidate = $newline + 1;

            if ($this->confirms($stream, $candidate)) {
                return $candidate;
            }

            $search = $candidate;
        }
    }

    /**
     * Whether the $confirmations records starting at $position each tokenise, strictly, to $expectedFields fields -
     * an enclosure may only open a field and must close it, so text cut out of a quoted value, which fgetcsv would
     * read as a record, is rejected at its first stray enclosure. Blank lines are skipped; a stream that ends first
     * confirms when at least one record was read and every one of them matched.
     *
     * @param int<0, max> $position
     */
    public function confirms(SourceStream $stream, int $position): bool
    {
        $separator = $this->dialect->separator;
        $enclosure = $this->dialect->enclosure;
        $escape = $this->dialect->escape === $enclosure ? '' : $this->dialect->escape;
        $state = self::FIELD_START;
        $fields = 0;
        $confirmed = 0;
        $escaped = false;

        while (($chunk = $stream->read(self::CHUNK, $position)) !== '') {
            $length = strlen($chunk);
            $position += $length;

            for ($i = 0; $i < $length; $i++) {
                $byte = $chunk[$i];

                if ($state === self::ENCLOSED) {
                    if ($escaped) {
                        $escaped = false;
                    } elseif ($escape !== '' && $byte === $escape) {
                        $escaped = true;
                    } elseif ($byte === $enclosure) {
                        $state = self::AFTER_ENCLOSURE;
                    }

                    continue;
                }

                if ($byte === "\n") {
                    if ($state === self::FIELD_START && $fields === 0) {
                        continue;
                    }

                    if (($fields + 1) !== $this->expectedFields) {
                        return false;
                    }

                    if (++$confirmed >= $this->confirmations) {
                        return true;
                    }

                    $state = self::FIELD_START;
                    $fields = 0;

                    continue;
                }

                if ($byte === $separator) {
                    $state = self::FIELD_START;
                    $fields++;

                    continue;
                }

                if ($state === self::AFTER_ENCLOSURE) {
                    if ($byte === $enclosure) {
                        $state = self::ENCLOSED;
                    } elseif ($byte !== "\r") {
                        return false;
                    }

                    continue;
                }

                if ($byte === $enclosure) {
                    if ($state === self::UNENCLOSED) {
                        return false;
                    }

                    $state = self::ENCLOSED;

                    continue;
                }

                if ($state === self::FIELD_START && !str_contains($this->blanks, $byte)) {
                    $state = self::UNENCLOSED;
                }
            }
        }

        if ($state === self::ENCLOSED) {
            return false;
        }

        if ($state !== self::FIELD_START || $fields > 0) {
            if (($fields + 1) !== $this->expectedFields) {
                return false;
            }

            $confirmed++;
        }

        return $confirmed > 0;
    }
}
