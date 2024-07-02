<?php

namespace Flow\ETL\Adapter\GoogleAnalytics;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Google\Analytics\Admin\V1beta\AccountSummary;
use Google\Analytics\Admin\V1beta\AnalyticsAdminServiceClient;
use Google\Analytics\Admin\V1beta\PropertySummary;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use function Flow\ETL\DSL\rows;

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
        $list = $this->client->listAccountSummaries(['pageSize' => $this->pageSize]);

        /** @var AccountSummary $account */
        foreach ($list->iterateAllElements() as $accountSummary) {
            $signal = yield rows(ga_account_summary_to_row($accountSummary));
            $this->incrementReturnedRows();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                return;
            }
        }

        while ($list->getPage()->hasNextPage()) {
            $list = $this->client->listAccountSummaries(['pageSize' => $this->pageSize, 'pageToken' => $list->getPage()->getNextPageToken()]);

            foreach ($list->iterateAllElements() as $accountSummary) {
                $signal = yield rows(ga_account_summary_to_row($accountSummary));
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }
        }
    }
}