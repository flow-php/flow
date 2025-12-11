<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\FunctionParameterMode;

enum ArgumentMode : int
{
    case IN = FunctionParameterMode::FUNC_PARAM_IN;
    case INOUT = FunctionParameterMode::FUNC_PARAM_INOUT;
    case OUT = FunctionParameterMode::FUNC_PARAM_OUT;
    case VARIADIC = FunctionParameterMode::FUNC_PARAM_VARIADIC;
}
