<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use DateTimeInterface;
use Stringable;
use Throwable;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_scalar;
use function json_encode;
use function mb_strlen;
use function mb_substr;
use function sprintf;

use const JSON_INVALID_UTF8_SUBSTITUTE;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

/**
 * @type CorruptedRecordShape = array{
 *     uri: ?string,
 *     offset: int,
 *     line: ?int,
 *     reason: string,
 *     record: string,
 *     fields: null|list<string>
 * }
 */
final class CorruptedRecordException extends RuntimeException
{
    private const int SHOWN_RECORD_LENGTH = 120;

    /**
     * @param null|string $uri null for a source that is not a file
     * @param int $offset the record's byte offset in a text file, its 1-based row number in a sheet or an array
     * @param null|list<string> $fields the cells a grid reader split the record into; null for keyed formats
     */
    public function __construct(
        public readonly ?string $uri,
        public readonly int $offset,
        public readonly ?int $lineNumber,
        public readonly string $reason,
        public readonly string $record,
        public readonly ?array $fields = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            sprintf(
                'Corrupted record at offset %d%s of %s: %s: "%s". Route corrupted records to a loader with '
                . '->withCorruptedRecords($loader).',
                $offset,
                $lineNumber === null ? '' : ", line {$lineNumber},",
                $uri ?? 'the source',
                $reason,
                mb_strlen($record) > self::SHOWN_RECORD_LENGTH
                    ? mb_substr($record, 0, self::SHOWN_RECORD_LENGTH) . '…'
                    : $record,
            ),
            0,
            $previous,
        );
    }

    /**
     * The inverse of toRecord().
     *
     * @param array<array-key, mixed> $record the shape of toRecord()
     *
     * @throws InvalidArgumentException
     */
    public static function fromRecord(array $record, ?Throwable $previous = null): self
    {
        $shape = type_structure([
            'uri' => type_optional(type_string()),
            'offset' => type_integer(),
            'line' => type_optional(type_integer()),
            'reason' => type_string(),
            'record' => type_string(),
            'fields' => type_optional(type_list(type_string())),
        ]);

        if (!$shape->isValid($record)) {
            throw new InvalidArgumentException(
                'A corrupted record must hold uri (?string), offset (int), line (?int), reason (string), record (string) '
                . 'and fields (?list<string>)',
            );
        }

        return new self(
            $record['uri'],
            $record['offset'],
            $record['line'],
            $record['reason'],
            $record['record'],
            $record['fields'],
            $previous,
        );
    }

    /**
     * The row a routed corrupted record becomes.
     *
     * @return CorruptedRecordShape
     */
    public function toRecord(): array
    {
        return [
            'uri' => $this->uri,
            'offset' => $this->offset,
            'line' => $this->lineNumber,
            'reason' => $this->reason,
            'record' => $this->record,
            'fields' => $this->fields,
        ];
    }

    /**
     * A grid record: its cells in their string form, and its text the source's own when it holds one, else the cells
     * as a JSON list.
     *
     * @param array<array-key, mixed> $cells
     */
    public static function fromCells(
        ?string $uri,
        int $offset,
        ?int $lineNumber,
        string $reason,
        array $cells,
        ?string $record = null,
    ): self {
        $fields = array_values(array_map(self::cell(...), $cells));

        return new self($uri, $offset, $lineNumber, $reason, $record ?? self::json($fields), $fields);
    }

    /**
     * A keyed record: its text the source's own when it holds one, else the record as a JSON object.
     *
     * @param array<array-key, mixed> $values
     */
    public static function fromValues(
        ?string $uri,
        int $offset,
        ?int $lineNumber,
        string $reason,
        array $values,
        ?string $record = null,
    ): self {
        return new self($uri, $offset, $lineNumber, $reason, $record ?? self::json($values));
    }

    private static function cell(mixed $cell): string
    {
        return match (true) {
            $cell === null => '',
            $cell instanceof DateTimeInterface => $cell->format(DateTimeInterface::ATOM),
            is_scalar($cell), $cell instanceof Stringable => (string) $cell,
            default => self::json($cell),
        };
    }

    private static function json(mixed $value): string
    {
        return (string) json_encode(
            $value,
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE,
        );
    }
}
