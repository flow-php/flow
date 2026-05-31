<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration\Memory;

use Flow\ETL\Adapter\Seal\Tests\AbstractSealLoaderTestCase;
use Flow\ETL\Adapter\Seal\Tests\Backend\MemoryBackend;

final class SealLoaderTest extends AbstractSealLoaderTestCase
{
    use MemoryBackend;
}
