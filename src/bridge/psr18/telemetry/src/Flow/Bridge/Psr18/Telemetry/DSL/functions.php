<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\DSL;

use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\Telemetry\Telemetry;
use Psr\Http\Client\ClientInterface;

#[DocumentationDSL(module: Module::PSR18_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr18_traceable_client(ClientInterface $client, Telemetry $telemetry): PSR18TraceableClient
{
    return new PSR18TraceableClient($client, $telemetry);
}
