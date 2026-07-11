<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;

/**
 * One schema section of a Floe file - the frames written between schema
 * changes within a single write session. Appends always start a new section,
 * even when they reuse the previous schema.
 */
final readonly class Section
{
    public function __construct(
        public int $offset,
        public int $schemaId,
        public int $rowCount,
    ) {}

    /**
     * @param array<string, mixed> $data
     *
     * @throws FloeException
     */
    public static function fromArray(array $data): self
    {
        try {
            $data = type_structure([
                'offset' => type_integer(),
                'schemaId' => type_integer(),
                'rowCount' => type_integer(),
            ])->assert($data);
        } catch (InvalidTypeException $e) {
            throw new FloeException('Floe footer section is malformed: ' . $e->getMessage(), 0, $e);
        }

        return new self($data['offset'], $data['schemaId'], $data['rowCount']);
    }

    /**
     * @return array{offset: int, schemaId: int, rowCount: int}
     */
    public function normalize(): array
    {
        return ['offset' => $this->offset, 'schemaId' => $this->schemaId, 'rowCount' => $this->rowCount];
    }
}
