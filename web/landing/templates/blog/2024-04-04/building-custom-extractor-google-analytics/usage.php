<?php

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Adapter\GoogleAnalytics\AccountSummariesExtractor;

// $client = new AnalyticsAdminServiceClient([
//     'credentials' => $credentials
// ]);

df()
    ->read(new AccountSummariesExtractor($client))
    ->limit(2)
    ->collect()
    ->write(to_output())
    ->run();

// Output
// +--------------------+----------------------+--------------+----------------------+
// |            account |                 name |  displayName |    propertySummaries |
// +--------------------+----------------------+--------------+----------------------+
// | accounts/111111111 | accountSummaries/111 | norbert.tech | [{"property":"proper |
// | accounts/222222222 | accountSummaries/222 |     aeon-php | [{"property":"proper |
// +--------------------+----------------------+--------------+----------------------+
