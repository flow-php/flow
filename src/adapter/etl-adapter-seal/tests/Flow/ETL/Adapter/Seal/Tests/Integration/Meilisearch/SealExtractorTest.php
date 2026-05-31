<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration\Meilisearch;

use Flow\ETL\Adapter\Seal\Tests\AbstractSealExtractorTestCase;
use Flow\ETL\Adapter\Seal\Tests\Backend\MeilisearchBackend;

final class SealExtractorTest extends AbstractSealExtractorTestCase
{
    use MeilisearchBackend;
}
