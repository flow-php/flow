<?php

declare(strict_types=1);

use Flow\ETL\DSL\To;
use Flow\ETL\DSL\XML;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

print "Reading XML dataset...\n";

(new Flow())
    ->read(XML::from(__FLOW_DATA__ . '/simple_items.xml', 'root/items/item'))
    ->write(To::output(false))
    ->run();
