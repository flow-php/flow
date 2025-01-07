<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use Rector\Validation\RectorAssert;

final readonly class NewObjectToFunction
{
    public function __construct(
        public string $className,
        public string $functionName,
    ) {
        RectorAssert::className($this->className);
        RectorAssert::functionName($this->functionName);
    }
}
