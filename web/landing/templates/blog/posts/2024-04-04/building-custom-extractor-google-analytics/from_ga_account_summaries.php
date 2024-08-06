<?php

use Flow\ETL\Adapter\GoogleAnalytics\AccountSummariesExtractor;
use Google\Analytics\Admin\V1beta\AnalyticsAdminServiceClient;

function from_ga_account_summaries(AnalyticsAdminServiceClient $client, int $page_size = 200) : AccountSummariesExtractor
{
    return new GoogleAnalyticsExtractor($client, $page_size);
}
