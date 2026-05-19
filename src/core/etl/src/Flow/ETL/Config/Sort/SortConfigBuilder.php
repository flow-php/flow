<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Sort\SortAlgorithms;

use function getenv;
use function ini_get;
use function is_string;

use const PHP_INT_MAX;

final class SortConfigBuilder
{
    public const int DEFAULT_SORT_MEMORY_PERCENTAGE = 70;

    private SortAlgorithms $algorithm = SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT;

    private string $filesystemProtocol = 'file';

    private ?Unit $sortMemoryLimit = null;

    public function algorithm(SortAlgorithms $algorithm): self
    {
        $this->algorithm = $algorithm;

        return $this;
    }

    public function build(): SortConfig
    {
        if ($this->sortMemoryLimit === null) {
            $sortMemory = getenv(SortConfig::SORT_MAX_MEMORY_ENV);

            if (is_string($sortMemory)) {
                $this->sortMemoryLimit = Unit::fromString($sortMemory);
            } else {
                $memoryLimit = ini_get('memory_limit');

                if ($memoryLimit === false || $memoryLimit === '-1') {
                    $this->sortMemoryLimit = Unit::fromBytes(PHP_INT_MAX);
                } else {
                    $this->sortMemoryLimit = Unit::fromString(
                        $memoryLimit,
                    )->percentage(self::DEFAULT_SORT_MEMORY_PERCENTAGE);
                }
            }
        }

        return new SortConfig($this->algorithm, $this->sortMemoryLimit, $this->filesystemProtocol);
    }

    public function filesystemProtocol(string $protocol): self
    {
        $this->filesystemProtocol = $protocol;

        return $this;
    }

    public function sortMemoryLimit(Unit $sortMemoryLimit): self
    {
        $this->sortMemoryLimit = $sortMemoryLimit;

        return $this;
    }
}
