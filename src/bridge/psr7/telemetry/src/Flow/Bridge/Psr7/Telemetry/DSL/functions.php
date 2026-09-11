<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry\DSL;

use Flow\Bridge\Psr7\Telemetry\RequestCarrier;
use Flow\Bridge\Psr7\Telemetry\ResponseCarrier;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;

#[DocumentationDSL(module: Module::PSR7_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr7_request_carrier(ServerRequestInterface $request): RequestCarrier
{
    return new RequestCarrier($request);
}

#[DocumentationDSL(module: Module::PSR7_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr7_response_carrier(ResponseInterface $response): ResponseCarrier
{
    return new ResponseCarrier($response);
}
