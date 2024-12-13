<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{data_frame, ref, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(
        from_xml(__DIR__ . '/input/dataset.xml')
            ->withXMLNodePath('users/user')
    )
    ->withEntry('id', ref('node')->domElementAttributeValue('id'))
    ->withEntry('name', ref('node')->xpath('name')->domElementValue())
    ->withEntry('active', ref('node')->xpath('active')->domElementValue())
    ->withEntry('email', ref('node')->xpath('email')->domElementValue())
    ->drop('node')
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
