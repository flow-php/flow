<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Abstraction;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class XMLAttribute
{
    public function __construct(
        public string $name,
        public string $value,
    ) {
        if (!\mb_strlen($name)) {
            throw new InvalidArgumentException('XMLAttribute name can not be empty');
        }
    }
}
