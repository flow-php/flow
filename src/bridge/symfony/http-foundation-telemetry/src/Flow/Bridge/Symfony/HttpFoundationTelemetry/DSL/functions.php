<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry\DSL;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\{RequestCarrier, ResponseCarrier};
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Symfony\Component\HttpFoundation\{Request, Response};

#[DocumentationDSL(module: Module::SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function symfony_request_carrier(Request $request) : RequestCarrier
{
    return new RequestCarrier($request);
}

#[DocumentationDSL(module: Module::SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function symfony_response_carrier(Response $response) : ResponseCarrier
{
    return new ResponseCarrier($response);
}
