<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function Flow\Types\DSL\type_string;
use function strlen;

final readonly class CSVDialect
{
    /**
     * @throws InvalidArgumentException when a character is not a single one (an escape may also be empty)
     */
    public function __construct(
        public string $separator,
        public string $enclosure,
        public string $escape,
    ) {
        self::assertParts($separator, $enclosure, $escape);
    }

    /**
     * The checks of a dialect whose parts may be left to detection.
     *
     * @throws InvalidArgumentException when a given character is not a single one (an escape may also be empty)
     */
    public static function assertParts(?string $separator, ?string $enclosure, ?string $escape): void
    {
        if ($separator !== null && strlen($separator) !== 1) {
            throw new InvalidArgumentException('Separator must be a single character');
        }

        if ($enclosure !== null && strlen($enclosure) !== 1) {
            throw new InvalidArgumentException('Enclosure must be a single character');
        }

        if ($escape !== null && strlen($escape) > 1) {
            throw new InvalidArgumentException('Escape must be empty or a single character');
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            type_string()->assert($data['separator'] ?? null),
            type_string()->assert($data['enclosure'] ?? null),
            type_string()->assert($data['escape'] ?? null),
        );
    }

    /**
     * @return array{separator: string, enclosure: string, escape: string}
     */
    public function normalize(): array
    {
        return ['separator' => $this->separator, 'enclosure' => $this->enclosure, 'escape' => $this->escape];
    }
}
