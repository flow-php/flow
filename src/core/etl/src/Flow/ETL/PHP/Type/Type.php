<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

/**
 * @template T
 */
interface Type
{
    public function isEqual(self $type) : bool;

    public function isValid(mixed $value) : bool;

    public function nullable() : bool;

    public function toString() : string;
}
