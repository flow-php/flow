<?php

namespace Flow\ETL\Adapter\GoogleAnalytics;

use Flow\ETL\Row;
use Google\Analytics\Admin\V1beta\AccountSummary;
use Google\Analytics\Admin\V1beta\AnalyticsAdminServiceClient;
use Google\Analytics\Admin\V1beta\PropertySummary;
use function Flow\ETL\DSL\{list_entry, row, str_entry, type_integer, type_list, type_string, type_structure};

function ga_account_summary_to_row(AccountSummary $accountSummary) : Row
{
    return row(
        str_entry('account', $accountSummary->getAccount()),
        str_entry('name', $accountSummary->getName()),
        str_entry('displayName', $accountSummary->getDisplayName()),
        list_entry(
            'propertySummaries',
            array_map(
                static fn(PropertySummary $propertySummary) => [
                    'property' => $propertySummary->getProperty(),
                    'displayName' => $propertySummary->getDisplayName(),
                    'propertyType' => $propertySummary->getPropertyType(),
                    'parent' => $propertySummary->getParent(),
                ],
                \iterator_to_array($accountSummary->getPropertySummaries())
            ),
            type_list(
                type_structure(
                    [
                        'property' => type_string(),
                        'displayName' => type_string(),
                        'propertyType' => type_integer(),
                        'parent' => type_string(),
                    ]
                )
            ),
        )
    );
}
