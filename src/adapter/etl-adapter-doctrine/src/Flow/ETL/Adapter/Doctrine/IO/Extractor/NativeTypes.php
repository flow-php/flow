<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Flow\Types\Type;

interface NativeTypes
{
    /**
     * @return null|Type<mixed> null when this adapter has no Flow type for the driver type
     */
    public function toFlowType(int|string|null $native): ?Type;
}
