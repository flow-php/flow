<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function max;

/**
 * What one sniff of one unit produced: the fold, the bytes it read and whether it reached the unit's end.
 */
final readonly class Sample
{
    /**
     * @param int<0, max> $bytes bytes the sniffed rows took as read, line endings included; 0 when unknown or in memory
     * @param bool $complete every row of the unit was observed
     */
    public function __construct(
        public SniffedColumns $columns,
        public int $bytes,
        public bool $complete,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($bytes < 0) {
            throw new InvalidArgumentException('Sampled bytes must not be negative');
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            SniffedColumns::fromArray(type_array()->assert($data['columns'] ?? null)),
            max(0, type_integer()->assert($data['bytes'] ?? null)),
            type_boolean()->assert($data['complete'] ?? null),
        );
    }

    /**
     * The sample of a unit sniffed in parts, in part order: the folds merged, the bytes summed, complete only when
     * every part was.
     */
    public function merge(self $other): self
    {
        return new self(
            $this->columns->merge($other->columns, true),
            $this->bytes + $other->bytes,
            $this->complete && $other->complete,
        );
    }

    /**
     * @return array{columns: array{columns: list<array{name: string, type: array<string, mixed>}>, rows: int}, bytes: int, complete: bool}
     */
    public function normalize(): array
    {
        return ['columns' => $this->columns->normalize(), 'bytes' => $this->bytes, 'complete' => $this->complete];
    }
}
