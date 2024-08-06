<?php

namespace Flow\ETL\Adapter\GoogleAnalytics;

use Flow\ETL\FlowContext;
use Google\Analytics\Admin\V1beta\AccountSummary;
use Google\Analytics\Admin\V1beta\AnalyticsAdminServiceClient;
use Google\Analytics\Admin\V1beta\PropertySummary;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;

final class AccountSummariesExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

    public function __construct(
        private readonly AnalyticsAdminServiceClient $client,
        private readonly int $pageSize = 200
    ) {
        if ($this->pageSize < 1 || $this->pageSize > 200) {
            throw new \Flow\ETL\Exception\InvalidArgumentException('Page size must be greater than 0 and lower than 200.');
        }
    }

    public function extract(FlowContext $context): \Generator
    {
        // TODO
    }
}