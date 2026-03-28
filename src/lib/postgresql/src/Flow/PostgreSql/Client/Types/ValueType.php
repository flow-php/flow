<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

enum ValueType : int
{
    case BIT = 1560;
    case BOOL = 16;

    case BOOL_ARRAY = 1000;
    case BPCHAR = 1042;  // blank-padded char

    case BYTEA = 17;

    case CHAR = 18;
    case CIDR = 650;

    case DATE = 1082;
    case FLOAT4 = 700;   // real
    case FLOAT4_ARRAY = 1021;
    case FLOAT8 = 701;   // double precision
    case FLOAT8_ARRAY = 1022;

    case INET = 869;
    case INT2 = 21;      // smallint
    case INT2_ARRAY = 1005;
    case INT4 = 23;      // integer
    case INT4_ARRAY = 1007;

    case INT8 = 20;      // bigint
    case INT8_ARRAY = 1016;
    case INTERVAL = 1186;

    case JSON = 114;
    case JSON_ARRAY = 199;
    case JSONB = 3802;
    case JSONB_ARRAY = 3807;
    case MACADDR = 829;
    case MACADDR8 = 774;
    case MONEY = 790;
    case NUMERIC = 1700;
    case OID = 26;
    case TEXT = 25;
    case TEXT_ARRAY = 1009;
    case TIME = 1083;
    case TIMESTAMP = 1114;
    case TIMESTAMPTZ = 1184;
    case TIMETZ = 1266;

    case UUID = 2950;
    case UUID_ARRAY = 2951;
    case VARBIT = 1562;
    case VARCHAR = 1043;
    case VARCHAR_ARRAY = 1015;

    case XML = 142;
}
