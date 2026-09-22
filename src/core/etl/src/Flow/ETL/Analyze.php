<?php

declare(strict_types=1);

namespace Flow\ETL;

final class Analyze
{
    private bool $collectColumnStatistics = false;

    private bool $collectSchema = false;

    private bool $collectSourceStatistics = false;

    public function __construct() {}

    public function collectColumnStatistics(): bool
    {
        return $this->collectColumnStatistics;
    }

    public function collectSchema(): bool
    {
        return $this->collectSchema;
    }

    public function collectSourceStatistics(): bool
    {
        return $this->collectSourceStatistics;
    }

    public function withColumnStatistics(): self
    {
        $this->collectColumnStatistics = true;

        return $this;
    }

    public function withSchema(): self
    {
        $this->collectSchema = true;

        return $this;
    }

    public function withSourceStatistics(): self
    {
        $this->collectSourceStatistics = true;

        return $this;
    }
}
