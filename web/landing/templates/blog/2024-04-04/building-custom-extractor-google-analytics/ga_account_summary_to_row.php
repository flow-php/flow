<?php

namespace Flow\ETL\Adapter\GoogleAnalytics;

use Flow\ETL\Row;
use Google\Analytics\Admin\V1beta\AccountSummary;
use Google\Analytics\Admin\V1beta\AnalyticsAdminServiceClient;
use Google\Analytics\Admin\V1beta\PropertySummary;
use function Flow\ETL\DSL\{list_entry, row, str_entry, structure_element, type_integer, type_list, type_string, type_structure};

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
                        structure_element('property', type_string()),
                        structure_element('displayName', type_string()),
                        structure_element('propertyType', type_integer()),
                        structure_element('parent', type_string()),
                    ]
                )
            ),
        )
    );
}
