/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: placeholder */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_parse, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_parse_protobuf, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_pg_query_fingerprint, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_pg_query_normalize, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_pg_query_normalize_utility arginfo_pg_query_normalize

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_parse_plpgsql, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_split, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_scan, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_deparse, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, protobuf, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_deparse_opts, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, protobuf, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, pretty_print, _IS_BOOL, 0, "false")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, indent_size, IS_LONG, 0, "4")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, max_line_length, IS_LONG, 0, "80")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, trailing_newline, _IS_BOOL, 0, "false")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, commas_start_of_line, _IS_BOOL, 0, "false")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_pg_query_summary, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, options, IS_LONG, 0, "0")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, truncate_limit, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_FUNCTION(pg_query_parse);
ZEND_FUNCTION(pg_query_parse_protobuf);
ZEND_FUNCTION(pg_query_fingerprint);
ZEND_FUNCTION(pg_query_normalize);
ZEND_FUNCTION(pg_query_normalize_utility);
ZEND_FUNCTION(pg_query_parse_plpgsql);
ZEND_FUNCTION(pg_query_split);
ZEND_FUNCTION(pg_query_scan);
ZEND_FUNCTION(pg_query_deparse);
ZEND_FUNCTION(pg_query_deparse_opts);
ZEND_FUNCTION(pg_query_summary);

static const zend_function_entry ext_functions[] = {
    ZEND_FE(pg_query_parse, arginfo_pg_query_parse)
    ZEND_FE(pg_query_parse_protobuf, arginfo_pg_query_parse_protobuf)
    ZEND_FE(pg_query_fingerprint, arginfo_pg_query_fingerprint)
    ZEND_FE(pg_query_normalize, arginfo_pg_query_normalize)
    ZEND_FE(pg_query_normalize_utility, arginfo_pg_query_normalize_utility)
    ZEND_FE(pg_query_parse_plpgsql, arginfo_pg_query_parse_plpgsql)
    ZEND_FE(pg_query_split, arginfo_pg_query_split)
    ZEND_FE(pg_query_scan, arginfo_pg_query_scan)
    ZEND_FE(pg_query_deparse, arginfo_pg_query_deparse)
    ZEND_FE(pg_query_deparse_opts, arginfo_pg_query_deparse_opts)
    ZEND_FE(pg_query_summary, arginfo_pg_query_summary)
    ZEND_FE_END
};
