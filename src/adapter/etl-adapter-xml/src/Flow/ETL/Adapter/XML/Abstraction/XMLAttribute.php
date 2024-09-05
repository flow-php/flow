<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Abstraction;

use Flow\ETL\Exception\InvalidArgumentException;

final class XMLAttribute
{
    public function __construct(
        public readonly string $name,
        public readonly string $value,
    ) {
        if (!\mb_strlen($name)) {
            throw new InvalidArgumentException('XMLAttribute name can not be empty');
        }
    }
}
