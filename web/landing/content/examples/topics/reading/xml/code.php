<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{data_frame, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(
        from_xml(__DIR__ . '/data/orders.xml')
            ->withXMLNodePath('rows/row')
    )
    ->withEntry('id', ref('node')->xpath('order_id')->domElementValue())
    ->withEntry('seller_id', ref('node')->xpath('seller_id')->domElementValue())
    ->withEntry('created_at', ref('node')->xpath('created_at')->domElementValue())
    ->withEntry('cancelled_at', ref('node')->xpath('cancelled_at')->domElementValue())
    ->withEntry('discount', ref('node')->xpath('discount')->domElementValue())
    ->drop('node')
    ->select('id', 'created_at', 'discount')
    ->limit(3)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
