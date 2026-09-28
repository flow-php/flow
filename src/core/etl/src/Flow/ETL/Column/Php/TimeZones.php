<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use DateTimeZone;

final class TimeZones
{
    /**
     * @var array<string, DateTimeZone>
     */
    private array $cache = [];

    public function get(string $name): DateTimeZone
    {
        return $this->cache[$name] ??= new DateTimeZone($name);
    }
}
