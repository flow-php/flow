<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;

interface PhpColumnBuilder
{
    /**
     * @param mixed $physical null is validity 0
     */
    public function appendPhysical(mixed $physical): void;

    /**
     * @param list<mixed> $physicals
     * @param null|int $nullCount the nulls among $physicals when the caller already counted them
     */
    public function appendPhysicals(array $physicals, ?int $nullCount = null): void;

    public function count(): int;

    public function finish(): Column;
}
