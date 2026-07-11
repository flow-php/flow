<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Sort\SortAlgorithms;

final class SortConfigBuilder
{
    private SortAlgorithms $algorithm = SortAlgorithms::EXTERNAL_SORT;

    private string $filesystemProtocol = 'file';

    public function algorithm(SortAlgorithms $algorithm): self
    {
        $this->algorithm = $algorithm;

        return $this;
    }

    public function build(): SortConfig
    {
        return new SortConfig($this->algorithm, $this->filesystemProtocol);
    }

    public function filesystemProtocol(string $protocol): self
    {
        $this->filesystemProtocol = $protocol;

        return $this;
    }
}
