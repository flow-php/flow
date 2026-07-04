<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema\Definition;

final readonly class MismatchedDefinition
{
    /**
     * @param Definition<mixed> $expected
     * @param Definition<mixed> $given
     */
    public function __construct(
        private Definition $expected,
        private Definition $given,
    ) {}

    /**
     * @return Definition<mixed>
     */
    public function expected(): Definition
    {
        return $this->expected;
    }

    /**
     * @return Definition<mixed>
     */
    public function given(): Definition
    {
        return $this->given;
    }
}
