<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;

use function array_intersect_key;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;

final readonly class Statistics
{
    /**
     * @param int $rows rows the file holds
     * @param int $byteSize uncompressed bytes of the data frames, header and footer excluded
     *
     * @throws FloeException
     */
    public function __construct(
        public int $rows,
        public int $byteSize,
    ) {
        if ($rows < 0) {
            throw new FloeException('Floe statistics rows must not be negative, given: ' . $rows);
        }

        if ($byteSize < 0) {
            throw new FloeException('Floe statistics byte size must not be negative, given: ' . $byteSize);
        }
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws FloeException
     */
    public static function fromArray(array $data): self
    {
        try {
            $data = type_structure([
                'rows' => type_integer(),
                'byteSize' => type_integer(),
            ])->assert(array_intersect_key($data, ['rows' => true, 'byteSize' => true]));
        } catch (InvalidTypeException $e) {
            throw new FloeException('Floe footer statistics are malformed: ' . $e->getMessage(), 0, $e);
        }

        return new self($data['rows'], $data['byteSize']);
    }

    /**
     * @return array{rows: int, byteSize: int}
     */
    public function normalize(): array
    {
        return [
            'rows' => $this->rows,
            'byteSize' => $this->byteSize,
        ];
    }
}
