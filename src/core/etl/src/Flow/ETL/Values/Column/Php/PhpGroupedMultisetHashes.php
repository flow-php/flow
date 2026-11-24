<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Php;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Values\Column\CanonicalEncoding;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Column\Physical\PhysicalFor;

use function array_map;
use function count;
use function Flow\Types\DSL\type_string;
use function hash;
use function max;
use function sprintf;
use function unpack;

final class PhpGroupedMultisetHashes implements GroupedMultisetHashes
{
    /**
     * @var list<MultisetDigest>
     */
    private array $digests = [];

    public function __construct(
        private readonly HashAlgorithm $algorithm,
        private readonly CanonicalEncoding $encoding = new CanonicalEncoding(),
    ) {}

    public function add(array $columns, Column $ids): void
    {
        /** @var list<int> $groups */
        $groups = $ids->physicals();

        if ($groups === []) {
            return;
        }

        $this->grow(max($groups));
        /** @var array<int, int<0, max>> $lowLow */
        $lowLow = [];
        /** @var array<int, int<0, max>> $lowHigh */
        $lowHigh = [];
        /** @var array<int, int<0, max>> $highLow */
        $highLow = [];
        /** @var array<int, int<0, max>> $highHigh */
        $highHigh = [];

        foreach ($this->encoding->named($columns, count($groups)) as $row => $bytes) {
            /** @var array{1: int<0, max>, 2: int<0, max>, 3: int<0, max>, 4: int<0, max>} $hash */
            $hash = unpack('N4', hash($this->algorithm->value, $bytes, true));
            $id = $groups[$row];
            $highHigh[$id] = ($highHigh[$id] ?? 0) + $hash[1];
            $highLow[$id] = ($highLow[$id] ?? 0) + $hash[2];
            $lowHigh[$id] = ($lowHigh[$id] ?? 0) + $hash[3];
            $lowLow[$id] = ($lowLow[$id] ?? 0) + $hash[4];
        }

        foreach ($lowLow as $id => $sum) {
            $this->digests[$id] = $this->digests[$id]->plus(MultisetDigest::ofHalfSums(
                $sum,
                $lowHigh[$id],
                $highLow[$id],
                $highHigh[$id],
            ));
        }
    }

    public function hashes(): Column
    {
        return new ScalarColumn(
            type_string(),
            (new PhysicalFor())->physical(type_string()),
            array_map(static fn(MultisetDigest $digest): string => $digest->hex(), $this->digests),
            0,
        );
    }

    public function absorbEarlier(GroupedMultisetHashes $earlier, array $into): void
    {
        $digests = $earlier instanceof self
            ? $earlier->digests
            : array_map(static fn(mixed $hex): MultisetDigest => MultisetDigest::fromHex(type_string()->assert(
                $hex,
            )), $earlier->hashes()->physicals());

        foreach ($into as $from => $to) {
            $this->grow($to);
            $this->digests[$to] = $this->digests[$to]->plus(
                $digests[$from] ?? throw new InvalidArgumentException(sprintf('Earlier group %d has no hash', $from)),
            );
        }
    }

    private function grow(int $id): void
    {
        while (count($this->digests) <= $id) {
            $this->digests[] = MultisetDigest::empty();
        }
    }
}
