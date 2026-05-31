<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration\Memory;

use Flow\ETL\Adapter\Seal\Tests\AbstractSealExtractorTestCase;
use Flow\ETL\Adapter\Seal\Tests\Backend\MemoryBackend;

final class SealExtractorTest extends AbstractSealExtractorTestCase
{
    use MemoryBackend;
}
