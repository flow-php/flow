<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\XML;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(XML::from(__FLOW_DATA__ . '/salaries.xml'))
    ->withEntry('months', ref('node')->xpath('/Salaries/Month'))
    ->withEntry('month', ref('months')->expand())
    ->withEntry('month_name', ref('month')->domNodeAttribute('name'))
    ->withEntry('departments', ref('month')->xpath('/Month/Department'))
    ->withEntry('department', ref('departments')->expand())
    ->withEntry('department_name', ref('department')->domNodeAttribute('name'))
    ->withEntry('department_salary', ref('department')->xpath('/Department/TotalSalary')->domNodeValue())
    ->drop('node', 'months', 'month', 'departments', 'department')
    ->groupBy(ref('month_name'))
    ->aggregate(sum(ref('department_salary')))
    ->rename('department_salary_sum', 'total_monthly_salaries')
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

print "Reading XML dataset...\n";

$flow->run();
