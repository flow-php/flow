<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Memory\Unit;

final class RepartitionProcessorMother
{
    /**
     * A limit no test reaches - the input is held and grouped in one pass.
     */
    public static function inMemory(
        References $by,
        RowsStorage $storage = new MemoryRowsStorage(),
    ): RepartitionProcessor {
        return self::with($by, $storage, Unit::fromGb(64));
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public static function with(
        References $by,
        RowsStorage $storage,
        Unit $memoryLimit,
        int $bucketsCount = 4,
    ): RepartitionProcessor {
        return new RepartitionProcessor(
            $by,
            new BucketingConfig($storage, $bucketsCount, 1, $memoryLimit),
            new NativePHPRandomValueGenerator(),
        );
    }
}
