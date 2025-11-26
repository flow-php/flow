/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: placeholder */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_parse, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_pg_query_fingerprint, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_pg_query_normalize, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_parse_plpgsql, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_split, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_scan, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(pg_query_parse);
ZEND_FUNCTION(pg_query_fingerprint);
ZEND_FUNCTION(pg_query_normalize);
ZEND_FUNCTION(pg_query_parse_plpgsql);
ZEND_FUNCTION(pg_query_split);
ZEND_FUNCTION(pg_query_scan);

static const zend_function_entry ext_functions[] = {
    ZEND_FE(pg_query_parse, arginfo_pg_query_parse)
    ZEND_FE(pg_query_fingerprint, arginfo_pg_query_fingerprint)
    ZEND_FE(pg_query_normalize, arginfo_pg_query_normalize)
    ZEND_FE(pg_query_parse_plpgsql, arginfo_pg_query_parse_plpgsql)
    ZEND_FE(pg_query_split, arginfo_pg_query_split)
    ZEND_FE(pg_query_scan, arginfo_pg_query_scan)
    ZEND_FE_END
};
