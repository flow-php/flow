<?php

declare(strict_types=1);

use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\XML;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(XML::from(__FLOW_DATA__ . '/simple_items.xml', 'root/items/item'))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

print "Reading XML dataset...\n";

$flow->run();
