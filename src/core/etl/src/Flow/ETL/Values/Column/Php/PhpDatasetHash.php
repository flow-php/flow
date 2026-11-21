<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Php;

use Flow\ETL\Values\Column\CanonicalEncoding;
use Flow\ETL\Values\Column\DatasetHash;
use Flow\ETL\Values\Column\MultisetDigest;

use function hash;
use function unpack;

final class PhpDatasetHash implements DatasetHash
{
    private MultisetDigest $digest;

    public function __construct(
        private readonly CanonicalEncoding $encoding = new CanonicalEncoding(),
    ) {
        $this->digest = MultisetDigest::empty();
    }

    public function add(array $columns, int $count): void
    {
        $lowLow = 0;
        $lowHigh = 0;
        $highLow = 0;
        $highHigh = 0;

        foreach ($this->encoding->named($columns, $count) as $bytes) {
            /** @var array{1: int<0, max>, 2: int<0, max>, 3: int<0, max>, 4: int<0, max>} $halves */
            $halves = unpack('N4', hash('xxh128', $bytes, true));
            $highHigh += $halves[1];
            $highLow += $halves[2];
            $lowHigh += $halves[3];
            $lowLow += $halves[4];
        }

        $this->digest = $this->digest->plus(MultisetDigest::ofHalfSums($lowLow, $lowHigh, $highLow, $highHigh));
    }

    public function digest(): MultisetDigest
    {
        return $this->digest;
    }

    public function absorb(MultisetDigest $digest): void
    {
        $this->digest = $this->digest->plus($digest);
    }
}
